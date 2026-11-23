//! Answer a request for one authorized prepared block on the requesting peer's task.

use std::task::{Context, Poll};

use futures::future::{ready, Either, Ready};
use tower::Service;

use zakura_chain::{block::Hash, chain_tip::ChainTip};
use zakura_network::{InventoryResponse, Request, Response};
use zakura_node_services::block_relay::ServedBlock;
use zakura_rpc::PendingBlockRegistry;

/// The current tip takes precedence over a prepared body under the same header hash.
pub(super) fn can_serve(body: &ServedBlock, tip: &impl ChainTip, hash: Hash) -> bool {
    body.pending_send
        .as_ref()
        .is_some_and(|permit| permit.prepared_bytes().is_some())
        && tip.best_tip_hash() != Some(hash)
}

/// Keeps the normal inbound buffer's admission check for every request, including cache hits.
/// A peer asking for the block just announced can read its authorized bytes without waiting
/// for the buffer's worker to run alongside the local commit.
#[derive(Clone)]
pub(crate) struct PreparedReads<S, T> {
    buffer: S,
    pending: PendingBlockRegistry,
    tip: T,
}

impl<S, T> PreparedReads<S, T> {
    pub(crate) fn new(buffer: S, pending: PendingBlockRegistry, tip: T) -> Self {
        Self {
            buffer,
            pending,
            tip,
        }
    }
}

impl<S, T> Service<Request> for PreparedReads<S, T>
where
    S: Service<Request, Response = Response> + Clone,
    T: ChainTip,
{
    type Response = Response;
    type Error = S::Error;
    type Future = Either<Ready<Result<Response, S::Error>>, S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.buffer.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let hashes = match &request {
            Request::BlocksByHash(hashes) | Request::BlocksByHashFrom { hashes, .. }
                if hashes.len() == 1 =>
            {
                hashes
            }
            _ => return Either::Right(self.buffer.call(request)),
        };
        let hash = hashes[0];
        if let Some(body) = self
            .pending
            .get_for_peer(hash)
            .filter(|body| can_serve(body, &self.tip, hash))
        {
            // Buffer clones own no reservation. Drop this handle's unused permit now so other
            // peers have exactly the same capacity as after a request through the worker.
            self.buffer = self.buffer.clone();
            metrics::counter!("mining.pending_registry.prepared_peer_replies").increment(1);
            let response = InventoryResponse::Available((body.into(), None));
            return Either::Left(ready(Ok(Response::Blocks(vec![response]))));
        }
        Either::Right(self.buffer.call(request))
    }
}
