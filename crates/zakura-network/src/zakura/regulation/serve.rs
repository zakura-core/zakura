//! Serving: one path for every request row, from admission to the ending.
//!
//! A reactor implements [`Produce`] for a request row: the response cap, one
//! `produce` step, and the ending to send after a local failure.
//! [`Serve`] owns everything else.
//!
//! 1. **Admission never waits.** [`Serve::admit`] counts one commitment for
//!    the session and queues the request. The reader keeps reading, so no
//!    stream layout can trap responses behind waiting requests.
//! 2. **The serving task waits instead.** For each queued request, in order,
//!    it takes the peer's output bytes and the node's output bytes for the
//!    whole response cap, then a peer execution slot and a node execution
//!    slot. Then it spawns `produce`.
//! 3. **`produce` never waits for the peer.** Its [`ResponseSink`] queues
//!    frames against output bytes that are already granted. `produce` gives
//!    back its execution slots as soon as it returns.
//! 4. **Output is ordered.** Each session's writer sends whole responses in
//!    admission order. A response's output grants return when its last frame
//!    finishes its transport write.
//! 5. **Every admitted request ends exactly once.** If `produce` returns
//!    without an ending, Serve queues [`Produce::local_failure`]'s ending. The
//!    connection stays open. Only an ending that cannot be queued retires the
//!    session.
//!
//! # Commitments and the margin
//!
//! A peer may have `limit` requests open, where `limit` is the highest limit
//! this node advertised on the connection. A commitment is released when its
//! ending enters the ordered output, before the ending is transmitted. A
//! conformant peer sends its next request only after it receives an ending,
//! so this node's tally never exceeds `limit` for a conformant peer.
//!
//! Serve still acts only above a proven margin. Even if the release happened
//! when the ending's write completed, the tally would include at most `limit`
//! further requests, one for each ending in transit. So:
//!
//! - up to `limit` open requests are normal;
//! - `limit + 1 ..= 2 × limit` are served and counted in
//!   `zakura.p2p.serve.over_limit`, never acted on;
//! - request `2 × limit + 1` is a protocol violation.
//!
//! Commitments are counted per session. A retired session's jobs may still be
//! running after the peer reconnects, and they never count against the new
//! session. They still hold the peer's execution and output budgets, which
//! the peer's sessions share.
//!
//! # Cancellation
//!
//! Cancellation never aborts `produce`. It marks the [`WorkLease`] cancelled,
//! and the execution slots stay held until `produce` and every lease clone
//! drop, so a blocking operation that is still running keeps its capacity.
//!
//! # Properties and their tests
//!
//! | Property | Test |
//! | --- | --- |
//! | Parts, then exactly one ending | `a_request_ends_with_its_parts_then_one_ending` |
//! | Whole responses in admission order | `output_is_whole_responses_in_admission_order` |
//! | Running `produce` steps never exceed node slots | `peak_running_produce_steps_stay_within_node_slots` |
//! | `limit` requests re-sent at each ending never fault | `exactly_limit_open_requests_never_fault_when_resent_at_each_ending` |
//! | Up to `2 × limit` served and traced; one more faults | `requests_within_twice_the_limit_are_served_and_traced_and_one_more_faults` |
//! | The highest advertised limit applies | `the_enforced_limit_is_the_highest_advertised` |
//! | A retired session's work never counts against the next | `a_retired_sessions_running_jobs_do_not_count_against_the_next_session` |
//! | Cancellation never aborts work or frees its slot early | `cancellation_lets_produce_finish_before_its_slot_returns`, `cancellation_keeps_the_node_slot_until_the_work_ends` |
//! | The node slot returns before the peer slot | `the_node_slot_returns_before_the_peer_slot` |
//! | A non-reading peer holds output but no execution | `a_non_reading_peer_holds_output_bytes_but_no_execution_slot` |
//! | Output returns when the last frame is written | `output_bytes_return_only_when_the_last_frame_is_written` |
//! | A stalled peer does not block another | `a_stalled_produce_on_one_peer_does_not_block_another` |
//! | A local failure ends the exchange; the connection stays | `a_local_failure_after_a_prefix_sends_the_failure_ending` |
//! | The ending frees the commitment before execution ends | `the_ending_frees_the_commitment_before_execution_ends` |
//! | The sink checks rows, frames, bytes, and the ending reserve | the `the_sink_*` and `a_*cap*` tests |
//! | Every budget returns after any operation sequence | `cancelling_a_session_frees_every_budget`, `operation_sequences_keep_every_bound` |

mod capacity;
mod lease;
mod push;
mod sink;

#[cfg(test)]
pub(crate) mod tests;

use std::{
    future::Future,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex, PoisonError,
    },
};

use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub(crate) use capacity::{ServeCapacity, ServeConfigError, ServeLimits};
pub(crate) use lease::WorkLease;
pub(crate) use push::{Push, PushPermit};
pub(crate) use sink::{Responded, ResponseCap, ResponseSink, SinkError, SinkProgress};

use capacity::{PeerBudgets, ServeMetrics};
use lease::ExecutionSlots;
pub(super) use sink::ending_reserve;
use sink::{ResponseFrame, SinkCore};

use super::OutputGrant;
use crate::zakura::{wire_codec::WireMessage, FramedSend, ZakuraPeerId};

/// A reactor's serving logic for one request row.
pub(crate) trait Produce: Send + Sync + 'static {
    /// The decoded request.
    type Request: Send + Sync + 'static;
    /// The reactor's message family.
    type Message: WireMessage<Error: std::fmt::Display> + Send + 'static;

    /// The most the response may carry. The requester reserves the same value.
    fn response_cap(&self, request: &Self::Request) -> ResponseCap;

    /// Produce the response.
    ///
    /// The only way to obtain [`Responded`] is [`ResponseSink::finish`], so a
    /// successful `produce` queues exactly one ending. Long operations check
    /// [`WorkLease::is_cancelled`] and move a lease clone into any blocking
    /// task, so the capacity stays held until that task ends.
    fn produce(
        &self,
        request: &Self::Request,
        lease: WorkLease,
        sink: ResponseSink<Self::Message>,
    ) -> impl Future<Output = Result<Responded, ServeEnd>> + Send;

    /// The ending for a response that a local failure cut short after `sent`.
    ///
    /// It must be a legal ending for `request` after `sent`. Serve reserves
    /// room for the largest ending, so it always fits.
    fn local_failure(&self, request: &Self::Request, sent: SinkProgress) -> Self::Message;
}

/// Why `produce` returned without an ending.
#[derive(Debug)]
pub(crate) enum ServeEnd {
    /// The lease was cancelled; the requester is gone.
    Cancelled,
    /// A local failure stopped the work. The peer is not at fault.
    LocalFault(String),
}

/// A peer broke the protocol while requesting.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum ServeViolation {
    /// The peer has more open requests than any conformant peer could have.
    #[error("{open} open requests exceed twice the advertised limit of {limit}")]
    OverCommitted {
        /// Open requests on this session, this one included.
        open: u32,
        /// The highest limit this node advertised on the connection.
        limit: u32,
    },
}

/// The open requests of one session, and the limit they are checked against.
#[derive(Debug)]
struct Commitments {
    open: AtomicU32,
    limit: AtomicU32,
}

/// One open request. Dropping it releases the commitment.
#[derive(Debug)]
pub(super) struct Commitment(Arc<Commitments>);

impl Drop for Commitment {
    fn drop(&mut self) {
        self.0.open.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The peer and node output bytes of one response.
#[derive(Debug)]
pub(super) struct ResponseGrants {
    // Field order is drop order: the node grant returns first, as with
    // execution slots.
    _node: OutputGrant,
    _peer: OutputGrant,
}

/// An admitted request that waits for capacity.
struct Job<R> {
    request: R,
    commitment: Commitment,
    frames: mpsc::UnboundedSender<ResponseFrame>,
}

/// Serving for one peer session and one request row.
///
/// Dropping it stops admission; queued requests still run unless the session
/// is cancelled.
pub(crate) struct Serve<P: Produce> {
    commitments: Arc<Commitments>,
    max_in_flight: u32,
    jobs: mpsc::UnboundedSender<Job<P::Request>>,
    order: mpsc::UnboundedSender<mpsc::UnboundedReceiver<ResponseFrame>>,
    metrics: Arc<ServeMetrics>,
}

impl<P: Produce> std::fmt::Debug for Serve<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Serve")
            .field("commitments", &self.commitments)
            .finish_non_exhaustive()
    }
}

impl<R> std::fmt::Debug for Job<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job").finish_non_exhaustive()
    }
}

impl ServeCapacity {
    /// Bind `produce` to one peer session.
    ///
    /// `advertised` is the limit this node advertised to the peer; the row's
    /// `max_in_flight` caps it. Sessions of one peer share its budgets, so
    /// reconnecting never adds capacity. `cancel` ends the session's serving:
    /// queued requests are dropped and running `produce` steps see their
    /// lease cancelled.
    pub(crate) fn session<P: Produce>(
        &self,
        produce: Arc<P>,
        peer: &ZakuraPeerId,
        advertised: u32,
        send: FramedSend,
        cancel: CancellationToken,
    ) -> Serve<P> {
        let (jobs, queued) = mpsc::unbounded_channel();
        let (order, responses) = mpsc::unbounded_channel();
        let serve = Serve {
            commitments: Arc::new(Commitments {
                open: AtomicU32::new(0),
                limit: AtomicU32::new(0),
            }),
            max_in_flight: self.max_in_flight,
            jobs,
            order,
            metrics: self.metrics.clone(),
        };
        serve.advertise(advertised);
        let dispatch = Dispatch {
            produce,
            capacity: self.clone(),
            peer: self.peer(peer),
            cancel: cancel.clone(),
        };
        tokio::spawn(dispatch.run(queued));
        tokio::spawn(write_in_order(responses, send, cancel));
        serve
    }
}

impl<P: Produce> Serve<P> {
    /// Admit `request` without waiting.
    ///
    /// Returns an error only for a protocol violation: more open requests
    /// than any conformant peer could have. See the module docs for the
    /// margin.
    pub(crate) fn admit(&self, request: P::Request) -> Result<(), ServeViolation> {
        let open = self.commitments.open.fetch_add(1, Ordering::AcqRel) + 1;
        let commitment = Commitment(self.commitments.clone());
        let limit = self.commitments.limit.load(Ordering::Acquire);
        if open > limit.saturating_mul(2) {
            return Err(ServeViolation::OverCommitted { open, limit });
        }
        if open > limit {
            self.metrics.over_limit(open, limit);
        }
        self.metrics.admitted();
        let (frames, response) = mpsc::unbounded_channel();
        // A closed channel means the session is cancelled. Dropping the job
        // releases its commitment.
        let _ = self.jobs.send(Job {
            request,
            commitment,
            frames,
        });
        let _ = self.order.send(response);
        Ok(())
    }

    /// Record a limit advertised to the peer on this connection.
    ///
    /// The enforced limit is the highest one advertised, capped by the row's
    /// `max_in_flight`: after a lowered advertisement, a conformant peer may
    /// still have the old limit's requests open.
    pub(crate) fn advertise(&self, limit: u32) {
        self.commitments
            .limit
            .fetch_max(limit.clamp(1, self.max_in_flight), Ordering::AcqRel);
    }

    /// Open requests on this session.
    #[cfg(test)]
    pub(crate) fn open(&self) -> u32 {
        self.commitments.open.load(Ordering::Acquire)
    }
}

/// The serving task of one session: waits for capacity, then spawns `produce`.
struct Dispatch<P> {
    produce: Arc<P>,
    capacity: ServeCapacity,
    peer: PeerBudgets,
    cancel: CancellationToken,
}

impl<P: Produce> Dispatch<P> {
    async fn run(self, mut queued: mpsc::UnboundedReceiver<Job<P::Request>>) {
        loop {
            let job = tokio::select! {
                biased;
                () = self.cancel.cancelled() => return,
                job = queued.recv() => match job {
                    Some(job) => job,
                    None => return,
                },
            };
            let waiting = self.capacity.metrics.waiting();
            let cap = self.produce.response_cap(&job.request);
            let Some((grants, slots)) = self.acquire(cap).await else {
                return;
            };
            drop(waiting);
            let task = Task {
                produce: self.produce.clone(),
                metrics: self.capacity.metrics.clone(),
                cancel: self.cancel.clone(),
            };
            let core = SinkCore::new(
                self.capacity.request,
                P::Message::RULES,
                cap,
                job.frames,
                Arc::new(grants),
                job.commitment,
            );
            tokio::spawn(task.run(job.request, core, slots));
        }
    }

    /// Take, in order, peer output, node output, peer execution, and node
    /// execution. Returns `None` if the session is cancelled first; nothing
    /// stays held.
    async fn acquire(&self, cap: ResponseCap) -> Option<(ResponseGrants, ExecutionSlots)> {
        let bytes = cap.output_bytes();
        let peer_output = &self.peer.output;
        let node_output = &self.capacity.node_output;
        let peer = self
            .wait("peer_output", peer_output.grant(peer_output.clamp(bytes)))
            .await?;
        let node = self
            .wait("node_output", node_output.grant(node_output.clamp(bytes)))
            .await?;
        let grants = ResponseGrants {
            _node: node,
            _peer: peer,
        };
        let peer = self
            .wait("peer_execution", self.peer.execution.reserve())
            .await?;
        let node = self
            .wait("node_execution", self.capacity.node_execution.reserve())
            .await?;
        Some((grants, ExecutionSlots::new(peer, node)))
    }

    async fn wait<T>(&self, bound: &'static str, acquire: impl Future<Output = T>) -> Option<T> {
        tokio::pin!(acquire);
        if let Some(ready) = futures::FutureExt::now_or_never(&mut acquire) {
            return Some(ready);
        }
        self.capacity.metrics.delayed(bound);
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => None,
            acquired = acquire => Some(acquired),
        }
    }
}

/// One spawned `produce` step and its ending.
struct Task<P> {
    produce: Arc<P>,
    metrics: Arc<ServeMetrics>,
    cancel: CancellationToken,
}

impl<P: Produce> Task<P> {
    async fn run(self, request: P::Request, core: SinkCore, slots: ExecutionSlots) {
        let active = self.metrics.active();
        let lease = WorkLease::new(slots);
        let core = Arc::new(Mutex::new(core));
        let produced = {
            let produced =
                self.produce
                    .produce(&request, lease.clone(), ResponseSink::new(core.clone()));
            tokio::pin!(produced);
            tokio::select! {
                produced = &mut produced => produced,
                () = self.cancel.cancelled() => {
                    lease.cancel();
                    produced.await
                }
            }
        };
        // Execution ends when the last lease clone drops.
        drop(lease);
        drop(active);
        let outcome = match produced {
            Ok(Responded { .. }) => "ending",
            Err(ServeEnd::Cancelled) => "cancelled",
            Err(ServeEnd::LocalFault(detail)) => {
                tracing::debug!(%detail, "serving failed locally; sending the failure ending");
                "local_fault"
            }
        };
        self.metrics.ended(outcome);
        let mut core = core.lock().unwrap_or_else(PoisonError::into_inner);
        if core.ended() || self.cancel.is_cancelled() {
            return;
        }
        let ending = self.produce.local_failure(&request, core.progress());
        if let Err(error) = core.push(&ending, true) {
            // The exchange cannot end, so the session cannot stay usable.
            tracing::warn!(%error, "serving could not queue a failure ending; retiring the session");
            self.cancel.cancel();
        }
    }
}

/// Write whole responses in admission order.
///
/// A response whose frames stop without an ending means its task ended
/// without one, so the exchange can never end: the session retires.
async fn write_in_order(
    mut responses: mpsc::UnboundedReceiver<mpsc::UnboundedReceiver<ResponseFrame>>,
    send: FramedSend,
    cancel: CancellationToken,
) {
    loop {
        let mut response = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            response = responses.recv() => match response {
                Some(response) => response,
                None => return,
            },
        };
        loop {
            let frame = tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                frame = response.recv() => frame,
            };
            let Some(ResponseFrame { frame, guard, ends }) = frame else {
                tracing::warn!("a served response ended without an ending; retiring the session");
                cancel.cancel();
                return;
            };
            let slot = tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                slot = send.reserve_guarded() => slot,
            };
            match slot {
                Ok(slot) => slot.send(frame, guard),
                Err(_) => {
                    cancel.cancel();
                    return;
                }
            }
            if ends {
                break;
            }
        }
    }
}
