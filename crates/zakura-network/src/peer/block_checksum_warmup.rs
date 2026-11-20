//! Precompute legacy `block` message checksums while their block adverts are in flight.
//!
//! A legacy message header carries a checksum of its whole body. Without precomputation, the
//! first connection asked for a new large block hashes it while the requesting peer waits. The
//! node is otherwise idle for about one round trip between writing a block's `inv` and receiving
//! the `getdata`, so the advertisement starts hashing the body on a blocking thread instead.

use std::{
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use indexmap::IndexSet;
use tokio::sync::Semaphore;
use tower::{Service, ServiceExt};
use tracing::Instrument;

use zakura_chain::block;
use zakura_node_services::{block_memory::BlockHandle, task::spawn_erased};

use crate::{
    protocol::{
        external::{BlockChecksums, MIN_SHARED_CHECKSUM_BODY_LEN},
        internal::InventoryResponse,
    },
    BoxError, Request, Response,
};

#[cfg(test)]
mod tests;

/// How long a warm-up waits for the inbound service to return the advertised block.
///
/// Peers request a block about one round trip after its advert. A slower lookup is unlikely to
/// finish before the first `getdata`, and only that request would have reused its result.
const BLOCK_LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// The most block checksums precomputed at once. Further advertisements are not warmed while
/// this many are in flight; their first send hashes the body as before.
///
/// Locally announced blocks arrive one per new tip, so this only bounds unexpected bursts.
const MAX_CONCURRENT_WARMUPS: usize = 2;

/// Wraps the legacy block advertiser. Each block advertisement also precomputes the advertised
/// block's message checksum into the [`BlockChecksums`] its connections share.
///
/// The body comes from the inbound service, through the same lookup that answers peers'
/// `getdata`, so the checksum is for the body the connections will send. The advertisement is
/// forwarded without waiting for the warm-up, and a failed warm-up only leaves the hashing to
/// the first send.
#[derive(Clone)]
pub(crate) struct BlockChecksumWarmup<A, I> {
    advertiser: A,
    inbound: I,
    checksums: BlockChecksums,
    warmups: Arc<Semaphore>,
}

impl<A, I> BlockChecksumWarmup<A, I> {
    /// Returns `advertiser`, wrapped to precompute checksums into `checksums` with blocks that
    /// `inbound` looks up.
    pub(crate) fn new(advertiser: A, inbound: I, checksums: BlockChecksums) -> Self {
        Self {
            advertiser,
            inbound,
            checksums,
            warmups: Arc::new(Semaphore::new(MAX_CONCURRENT_WARMUPS)),
        }
    }
}

impl<A, I> BlockChecksumWarmup<A, I>
where
    I: Service<Request, Response = Response, Error = BoxError> + Clone + Send + 'static,
    I::Future: Send + 'static,
{
    /// Starts precomputing the checksum of the block advertised by `request`, unless the block
    /// is known to be too small to share its checksum, or its checksum is already known.
    fn start_warmup(&self, request: &Request) {
        let block = match request {
            Request::AdvertiseBlock(block, _)
            | Request::AdvertiseBlockToAll(block)
            | Request::AdvertiseMinedBlock(block) => block,
            _ => return,
        };
        // An unknown size is checked once the block is found.
        if let Some(size) = block.size {
            // Widening u32 to usize is lossless on supported targets.
            let size = size as usize;
            if size < MIN_SHARED_CHECKSUM_BODY_LEN
                || self
                    .checksums
                    .get(block.hash)
                    .is_some_and(|(len, _)| len == size)
            {
                return;
            }
        }
        let Ok(permit) = self.warmups.clone().try_acquire_owned() else {
            metrics::counter!("peer.block_checksum.warmup_skipped").increment(1);
            return;
        };

        let inbound = self.inbound.clone();
        let checksums = self.checksums.clone();
        let hash = block.hash;
        spawn_erased(
            async move {
                let _permit = permit;
                let Some(block) = lookup_block(inbound, hash).await else {
                    return;
                };
                let hashed = tokio::task::spawn_blocking(move || checksums.precompute(&block));
                if let Err(error) = hashed.await {
                    tracing::debug!(?hash, %error, "block checksum precomputation failed");
                }
            }
            .in_current_span(),
        );
    }
}

/// Always as ready as the wrapped advertiser. Requests that are not block advertisements are
/// passed through without a warm-up.
impl<A, I> Service<Request> for BlockChecksumWarmup<A, I>
where
    A: Service<Request, Response = Response, Error = BoxError>,
    I: Service<Request, Response = Response, Error = BoxError> + Clone + Send + 'static,
    I::Future: Send + 'static,
{
    type Response = Response;
    type Error = BoxError;
    type Future = A::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.advertiser.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        self.start_warmup(&request);
        self.advertiser.call(request)
    }
}

/// Returns the block `hash` that `inbound` would send to a peer, if it has the block and answers
/// within [`BLOCK_LOOKUP_TIMEOUT`].
async fn lookup_block<I>(inbound: I, hash: block::Hash) -> Option<BlockHandle>
where
    I: Service<Request, Response = Response, Error = BoxError>,
{
    let lookup = inbound.oneshot(Request::BlocksByHash(IndexSet::from([hash])));
    let response = match tokio::time::timeout(BLOCK_LOOKUP_TIMEOUT, lookup).await {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            tracing::debug!(?hash, %error, "could not look up a block to precompute its checksum");
            return None;
        }
        Err(_elapsed) => {
            tracing::debug!(?hash, "block lookup for checksum precomputation timed out");
            return None;
        }
    };
    let Response::Blocks(blocks) = response else {
        return None;
    };
    blocks.into_iter().find_map(|block| match block {
        InventoryResponse::Available((body, _))
            if body
                .pending_send()
                .is_none_or(|permit| permit.attempt_checksum().is_none())
                && body.hash() == hash =>
        {
            Some(body.into_block())
        }
        _ => None,
    })
}
