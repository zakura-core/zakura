//! Per-session serving admission with exact live-range overlap checks.

use std::{collections::BTreeMap, sync::Arc};
use tokio_util::sync::CancellationToken;
use zakura_chain::block::Height;

use super::{
    serving::{Server, Source},
    wire::{Range, GET_BLOCKS},
};
use crate::zakura::{
    block_sync::{ZakuraBlockSyncConfig, MAX_BS_BLOCKS_PER_REQUEST, MAX_BS_RESPONSE_BYTES},
    regulation::{sizing, CompletionId, Completions, Serve, ServeCapacity},
    FramedSend, SinkReject, ZakuraPeerId,
};

/// One shared capacity pool for every production GetBlocks session.
#[derive(Debug)]
pub(crate) struct Serving {
    source: Arc<dyn Source>,
    capacity: ServeCapacity,
    max_blocks: u32,
    max_body_bytes: u32,
    advertised: u32,
}

impl Serving {
    pub(crate) fn new(source: Arc<dyn Source>, config: &ZakuraBlockSyncConfig) -> Self {
        // A smaller configured response cap must not inflate execution concurrency.
        let largest = Range {
            start: Height::MIN,
            count: MAX_BS_BLOCKS_PER_REQUEST,
        }
        .response_cap(MAX_BS_RESPONSE_BYTES)
        .output_bytes();
        // Allow one target RTT for production. This is a sizing assumption,
        // not a measured storage latency.
        let limits = sizing::serve_limits(largest, sizing::TARGET_RTT);
        Self {
            source,
            capacity: ServeCapacity::new("block-sync", &GET_BLOCKS, limits)
                .expect("shared sizing yields positive GetBlocks capacity limits"),
            max_blocks: config.advertised_max_blocks_per_response(),
            max_body_bytes: config.advertised_max_response_bytes(),
            advertised: config.advertised_max_inflight_requests(),
        }
    }

    pub(crate) fn session(
        &self,
        peer: &ZakuraPeerId,
        send: FramedSend,
        cancel: CancellationToken,
        connection: CancellationToken,
        close_cause: crate::zakura::CloseCause,
    ) -> ServingSession {
        let producer = Server::new(self.source.clone(), self.max_blocks, self.max_body_bytes)
            .expect("advertised serving limits are within the protocol bounds")
            .with_session(send.clone());
        ServingSession {
            serve: self.capacity.session(
                Arc::new(producer),
                peer,
                self.advertised,
                send,
                cancel,
                connection,
                close_cause,
            ),
            ranges: BTreeMap::new(),
            completed: Completions::default(),
        }
    }
}

pub(crate) struct ServingSession {
    serve: Serve<Server<dyn Source>>,
    ranges: BTreeMap<Height, (Height, CompletionId)>,
    completed: Completions,
}

impl ServingSession {
    /// Admit without waiting for storage or output capacity. Only malformed,
    /// overlapping, or excessive commitments are peer faults.
    pub(crate) fn admit(&mut self, start: Height, count: u32) -> Result<(), SinkReject> {
        let range = Range::new(start, count).map_err(SinkReject::protocol)?;
        self.remove_completed();
        let last = Height(start.0 + count - 1);
        if self
            .ranges
            .range(..=last)
            .next_back()
            .is_some_and(|(_, (end, _))| *end >= start)
        {
            return Err(SinkReject::protocol(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "GetBlocks overlaps a live range",
            )));
        }
        let completed = self.completed.track(u64::from(start.0));
        let id = completed.id();
        self.serve
            .admit_tracked(range, completed)
            .map_err(SinkReject::protocol)?;
        self.ranges.insert(start, (last, id));
        Ok(())
    }

    /// Drain under the ending-publication lock before reusing any live range.
    fn remove_completed(&mut self) {
        self.completed.drain(|id| {
            let start =
                Height(u32::try_from(id.key()).expect("GetBlocks completion keys are heights"));
            if self
                .ranges
                .get(&start)
                .is_some_and(|(_, current)| *current == id)
            {
                self.ranges.remove(&start);
            }
        });
    }
}

#[cfg(test)]
mod tests;
