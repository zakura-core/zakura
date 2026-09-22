//! One serving loop for every request that produces a single response.
//!
//! A service implements [`Serve`]: a response bound and one `produce` step.
//! [`ServeSession::serve`] owns everything else, in this order:
//!
//! 1. wait for a per-peer execution slot;
//! 2. wait for a per-peer output grant of `response_cap` bytes;
//! 3. wait for a node execution slot;
//! 4. spawn the work, which runs `produce` to completion;
//! 5. release both execution slots, then queue the response while holding
//!    only the output grant, which the transport releases after the write.
//!
//! Every wait races the stream's cancellation. The reader therefore waits,
//! instead of dropping a reply, when the peer has too much work in flight. A
//! peer that stops reading stops at its output bound without holding a node
//! slot, so it cannot starve other peers.
//!
//! Cancellation never aborts the work. It marks the [`WorkLease`] cancelled
//! and the execution slots stay held until `produce` returns, so a blocking
//! operation that is still running keeps its capacity.

mod admission;
mod lease;
mod response_sink;

#[cfg(test)]
pub(super) mod tests;

use std::{future::Future, sync::Arc};

use tokio_util::sync::CancellationToken;

use admission::{PeerServeBudgets, ServeAdmission};
pub(crate) use admission::{PeerServeLimits, ServeCapacity};
pub(crate) use lease::WorkLease;
pub(crate) use response_sink::{Responded, ResponseSink};

use super::{OutputByteBudget, SlotBudget, Verdict};
use crate::zakura::{FrameGuard, FramedSend, ZakuraPeerId};

/// A request kind that produces exactly one response frame.
pub(crate) trait Serve: Send + Sync + 'static {
    /// The decoded request.
    type Request: Send + 'static;

    /// Upper bound on the response frame's encoded bytes, header included.
    fn response_cap(&self, request: &Self::Request) -> u32;

    /// Produce the response.
    ///
    /// The only way to obtain [`Responded`] is [`ResponseSink::respond`], so a
    /// successful `produce` sends exactly one response. Long operations check
    /// [`WorkLease::is_cancelled`] and move a lease clone into any blocking
    /// task, so the capacity stays held until that task ends.
    fn produce(
        &self,
        request: Self::Request,
        lease: WorkLease,
        sink: ResponseSink,
    ) -> impl Future<Output = Result<Responded, ServeEnd>> + Send;
}

/// Why `produce` ended without a response.
#[derive(Debug)]
pub(crate) enum ServeEnd {
    /// The lease was cancelled; the requester is gone.
    Cancelled,
    /// A local fault stopped the work; the peer is not at fault.
    LocalFault(String),
}

/// One peer session's view of a [`Serve`] implementation.
#[derive(Debug)]
pub(crate) struct ServeSession<S> {
    serve: Arc<S>,
    node: SlotBudget,
    peer: PeerServeBudgets,
    send: FramedSend,
    cancel: CancellationToken,
}

impl<S: Serve> ServeSession<S> {
    /// Bind `serve` to one peer session. Sessions of the same peer share its
    /// budgets, so reconnecting does not add capacity.
    pub(crate) fn new(
        serve: Arc<S>,
        capacity: &ServeCapacity,
        peer: &ZakuraPeerId,
        send: FramedSend,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            serve,
            node: capacity.node(),
            peer: capacity.peer(peer),
            send,
            cancel,
        }
    }

    /// Admit `request`, then produce and send its response in a spawned task.
    ///
    /// Returns once the work is admitted, or once the session is cancelled.
    pub(crate) async fn serve(&self, request: S::Request) {
        let response_cap = self.serve.response_cap(&request);
        let admitted = ServeAdmission {
            node: &self.node,
            peer: &self.peer,
            cancel: &self.cancel,
        }
        .admit(response_cap)
        .await;
        let Some((execution, output)) = admitted else {
            return;
        };
        let serve = self.serve.clone();
        let send = self.send.clone();
        let cancel = self.cancel.clone();
        tokio::spawn(async move {
            let lease = WorkLease::new(execution);
            let sink = ResponseSink::new(response_cap);
            let produce = serve.produce(request, lease.clone(), sink);
            tokio::pin!(produce);
            let produced = tokio::select! {
                result = &mut produce => result,
                () = cancel.cancelled() => {
                    lease.cancel();
                    produce.await
                }
            };
            // Execution ends here; only the output grant waits for the peer.
            drop(lease);
            let verdict = match produced {
                Ok(responded) => {
                    let guard = FrameGuard::new(Arc::new(output));
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => Verdict::Drop { reason: "serve_cancelled" },
                        sent = send.send_with_guard(responded.into_frame(), guard) => match sent {
                            Ok(()) => Verdict::Continue,
                            Err(_) => Verdict::Drop { reason: "serve_stream_closed" },
                        },
                    }
                }
                Err(ServeEnd::Cancelled) => Verdict::Drop {
                    reason: "serve_cancelled",
                },
                Err(ServeEnd::LocalFault(detail)) => Verdict::LocalFault {
                    reason: "serve_failed",
                    detail,
                },
            };
            if let Err(error) = verdict.into_stream_result() {
                tracing::debug!(?error, "Zakura serving task ended without a response");
            }
        });
    }
}
