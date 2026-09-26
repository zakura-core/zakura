//! Per-session serving admission with exact live-range overlap checks.

use std::{collections::BTreeMap, sync::Arc};

use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use zakura_chain::block::Height;

use super::{
    serving::{Server, Source},
    wire::{Range, GET_BLOCKS},
};
use crate::zakura::{
    block_sync::{ZakuraBlockSyncConfig, MAX_BS_BLOCKS_PER_REQUEST, MAX_BS_RESPONSE_BYTES},
    regulation::{sizing, Serve, ServeCapacity},
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
    ) -> ServingSession {
        let producer = Server::new(self.source.clone(), self.max_blocks, self.max_body_bytes)
            .expect("advertised serving limits are within the protocol bounds")
            .with_session(send.clone());
        ServingSession {
            serve: self
                .capacity
                .session(Arc::new(producer), peer, self.advertised, send, cancel),
            ranges: BTreeMap::new(),
            completed: FuturesUnordered::new(),
        }
    }
}

pub(crate) struct ServingSession {
    serve: Serve<Server<dyn Source>>,
    ranges: BTreeMap<Height, (Height, watch::Receiver<bool>)>,
    completed: FuturesUnordered<BoxFuture<'static, (Height, watch::Receiver<bool>)>>,
}

impl ServingSession {
    /// Admit without waiting for storage or output capacity. Only malformed,
    /// overlapping, or excessive commitments are peer faults.
    pub(crate) fn admit(&mut self, start: Height, count: u32) -> Result<(), SinkReject> {
        let range = Range::new(start, count).map_err(SinkReject::protocol)?;
        // Each completion is removed once. No full scan of live ranges is needed.
        while let Some(Some((start, completed))) = self.completed.next().now_or_never() {
            if self
                .ranges
                .get(&start)
                .is_some_and(|(_, current)| current.same_channel(&completed))
            {
                self.ranges.remove(&start);
            }
        }
        let last = Height(start.0 + count - 1);
        while let Some((&previous, (previous_last, completed))) =
            self.ranges.range(..=last).next_back()
        {
            // The watch read synchronizes with publication of the ending. A peer
            // may already have received it before the completion future is polled.
            if *completed.borrow() {
                self.ranges.remove(&previous);
                continue;
            }
            if *previous_last >= start {
                return Err(SinkReject::protocol(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "GetBlocks overlaps a live range",
                )));
            }
            break;
        }
        let completed = self
            .serve
            .admit_tracked(range)
            .map_err(SinkReject::protocol)?;
        self.ranges.insert(start, (last, completed.clone()));
        self.completed.push(Box::pin(async move {
            let mut completed = completed;
            let _ = completed.wait_for(|done| *done).await;
            (start, completed)
        }));
        Ok(())
    }
}
