//! Runtime handles of one block-sync peer routine.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use zakura_chain::block;
use zakura_node_services::block_memory::{
    Admission, AdmissionFailure, DecodedBlockPool, MeasuredBlock, OwnedBlock,
};

use zakura_rt::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, watch, CancellationToken, Mutex,
    },
    time::{self, Instant},
};

use super::super::{
    admission::AdmissionSnapshot,
    events::RoutineToReactor,
    peer_registry::PeerRegistry,
    regulated::{live_requester::LiveRequester, session::ServingSession},
    reorder::BufferedBlockBody,
    sequencer_task::{charge_decoded_handoff, DecodedHandoffCharge, SequencedBody, SequencerView},
    state::{OutstandingBlockRange, RequestKey, ThroughputMeter, WriteOutcome},
    work_queue::{RequestWriteStatus, WorkQueue},
    BlockSyncMisbehavior, BlockSyncPeerSession, ZakuraPeerId, ZakuraTrace,
};
use super::{state::unreceived_heights, SinkReject};
use crate::zakura::{transport::ByteReservation, FramedRecv, ZakuraConnId};

/// The streams, shared owners, channels, and trace handles that one peer
/// routine uses to execute its decisions. `PeerIO` makes no download, status,
/// or liveness decision of its own.
pub(super) struct PeerIO {
    /// Authorizes every response read from `recv`. The transport precheck
    /// shares this owner, so authorization happens before body decoding.
    pub(super) requester: LiveRequester,
    /// Admits this peer's `GetBlocks` requests.
    pub(super) serving: ServingSession,
    pub(super) peer: ZakuraPeerId,
    pub(super) conn_id: ZakuraConnId,
    pub(super) source: zakura_header_chain::SourceId,
    pub(super) session: BlockSyncPeerSession,
    /// This peer's ordered stream-6 frame reader. Decoded in the routine's own
    /// task; inbound never flows through the reactor (per-peer routines inverted data flow).
    pub(super) recv: FramedRecv,
    /// Live write handles of the requests in the session's window.
    pub(super) writes: RequestWrites,
    /// Last sampled fill-stop time for each bounded reason label.
    pub(super) fill_stop_trace_at: BTreeMap<&'static str, Instant>,
    /// Generation this routine was spawned with; gates its registry writes (and
    /// its `Drop`) so a superseded routine (e.g. a session replacement before the
    /// old task's async Drop runs) cannot corrupt the live entry.
    pub(super) generation: u64,
    /// Reads the budget that `work` owns. Only queue transitions change it.
    pub(super) budget: crate::zakura::transport::BudgetReader,
    pub(super) work: Arc<WorkQueue>,
    pub(super) registry: Arc<PeerRegistry>,
    pub(super) received_throughput: Arc<Mutex<ThroughputMeter>>,
    /// The per-peer sequencer path. A full input stalls only this routine.
    pub(super) sequencer_input: mpsc::Sender<SequencedBody>,
    pub(super) sequencer_input_bytes: Arc<AtomicU64>,
    pub(super) sequencer_input_decoded_attributed_memory_bytes: Arc<AtomicU64>,
    /// This generation's registry flag, set while the routine is blocked on
    /// its sequencer handoff (see [`HandoffBlocked`]).
    pub(super) handoff_blocked: Arc<AtomicBool>,
    /// Admits decoded bodies before the routine retains them.
    pub(super) decoded_pool: DecodedBlockPool,
    /// Shared routine-to-reactor channel for serving, status, re-query, and misbehavior events.
    /// Bounded `try_send` prevents a busy reactor from stalling the transport decode loop.
    pub(super) routine_to_reactor: mpsc::Sender<RoutineToReactor>,
    pub(super) sequencer_view: watch::Receiver<SequencerView>,
    /// Cancellation token for the peer's service session.
    /// Disconnect, park, or shutdown triggers the token.
    /// The routine then exits and its `Drop` guard returns work.
    pub(super) cancel: CancellationToken,
    pub(super) trace: ZakuraTrace,
    /// Receives the routine's events and snapshots, in test and simulator
    /// builds.
    #[cfg(any(test, zakura_sim))]
    pub(super) observer: super::super::block_sync_observe::ObserverSlot,
    /// Frames taken from `recv` so far.
    #[cfg(any(test, zakura_sim))]
    pub(super) frames_judged: u64,
}

impl PeerIO {
    /// End the live exchange of a request whose write settled as `write`.
    pub(super) fn retire_authorization(
        &self,
        outstanding: &OutstandingBlockRange,
        write: WriteOutcome,
    ) {
        self.requester.retire(
            outstanding.request.start_height,
            write == WriteOutcome::Skipped,
        );
    }

    /// Withdraw every registry claim that a request leaving the window still
    /// holds. A replacement request's claim at the same height stays published.
    pub(super) fn withdraw_claims(&self, outstanding: &OutstandingBlockRange) {
        self.registry.remove_outstanding_for_owner(
            &self.peer,
            self.generation,
            outstanding.request.owner,
            outstanding
                .request
                .expected_blocks
                .iter()
                .map(|expected| expected.height),
        );
    }

    /// Withdraw the registry claim for one received height.
    pub(super) fn withdraw_claim(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
    ) {
        self.registry
            .remove_outstanding_for_owner(&self.peer, self.generation, owner, [height]);
    }

    /// End a retired request's exchange and withdraw its registry claims.
    pub(super) fn retire_request(&self, outstanding: &OutstandingBlockRange, write: WriteOutcome) {
        self.retire_authorization(outstanding, write);
        self.withdraw_claims(outstanding);
    }

    /// Release a request's reservations and return its unreceived heights to the
    /// queue, under its exact owner. Returns those heights.
    pub(super) fn return_unreceived(
        &self,
        outstanding: &OutstandingBlockRange,
        reason: &'static str,
    ) -> Vec<block::Height> {
        let unreceived: Vec<_> = unreceived_heights(outstanding).collect();
        let outcome = self
            .work
            .release_reserved_and_return_items_detailed_for_owner(
                outstanding.request.owner,
                unreceived.iter().copied(),
            );
        self.trace_work_returned(reason, outstanding, unreceived.len(), outcome);
        unreceived
    }

    pub(super) fn report_misbehavior(&self, reason: BlockSyncMisbehavior) {
        // This path only records misbehavior: observe and forward it, but never
        // cancel the session. Only a proven tampered body, reported by the
        // sequencer, disconnects (see `BlockSyncMisbehavior::proves_tampered_body`).
        metrics::counter!("sync.block.peer.violation").increment(1);
        // `Misbehavior` is best-effort: never block the routine. The reactor owns
        // action dispatch so attacker-triggered reports cannot bypass its reserved
        // control capacity.
        let _ = self
            .routine_to_reactor
            .try_send(RoutineToReactor::Misbehavior {
                peer: self.peer.clone(),
                reason,
            });
    }

    pub(super) fn download_floor(&self) -> block::Height {
        self.sequencer_view.borrow().download_floor
    }

    pub(super) fn record_received(&self, bytes: u64) {
        // Count accepted serialized block payloads, excluding transport framing.
        metrics::counter!("sync.block.payload.received.bytes").increment(bytes);
        if let Ok(mut meter) = self.received_throughput.lock() {
            meter.record(bytes);
        }
    }

    /// Read the admission inputs and publish the above-floor reservation that
    /// admission compares with the look-ahead limit.
    pub(super) fn admission_snapshot(&self, view: &SequencerView) -> AdmissionSnapshot {
        let (reserved_above_floor_bytes, reserved_above_floor_blocks) =
            self.work.reserved_above(view.download_floor);
        let snapshot = AdmissionSnapshot {
            download_floor: view.download_floor,
            verified_block_tip: view.verified_tip,
            reorder_buffered_bytes: view.reorder_buffered_bytes,
            reorder_buffered_blocks: view.reorder_len,
            applying_buffered_bytes: view.applying_buffered_bytes,
            applying_buffered_blocks: view.applying_len,
            sequencer_input_queued_bytes: self.sequencer_input_bytes.load(Ordering::Relaxed),
            in_flight_submission_bytes: view.in_flight_submission_bytes,
            decoded_resident_bytes: self
                .sequencer_input_decoded_attributed_memory_bytes
                .load(Ordering::Relaxed)
                .saturating_add(view.reorder_decoded_attributed_memory_bytes)
                .saturating_add(view.applying_decoded_attributed_memory_bytes),
            reserved_above_floor_bytes,
            reserved_above_floor_blocks,
            budget_available: self.budget.available(),
        };
        // This gauge records an admission input, so it changes only here. This
        // lossy cast is metrics-only; admission keeps the integer value.
        metrics::gauge!("sync.block.work.reserved_above_floor_bytes")
            .set(snapshot.reserved_above_floor_bytes as f64);
        snapshot
    }

    /// Reserve the sequencer slot for a body about to be decoded. A full input
    /// queue parks this routine; `published_free_slots` says whether other
    /// routines may be deferring the floor to it.
    pub(super) async fn reserve_body_decode_permit(
        &self,
        published_free_slots: bool,
    ) -> Result<mpsc::OwnedPermit<SequencedBody>, SinkReject> {
        let _read_pause = self.requester.pause_reading();
        let capacity_before = self.sequencer_input.capacity();
        let started = time::Instant::now();
        let closed = || SinkReject::local("block-sync sequencer body input closed");
        let permit = match self.sequencer_input.clone().try_reserve_owned() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(sequencer_input)) => {
                // The input queue is full, so this routine parks until the
                // sequencer drains a body.
                let _blocked = HandoffBlocked::start(self, published_free_slots);
                sequencer_input
                    .reserve_owned()
                    .await
                    .map_err(|_| closed())?
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(closed()),
        };
        self.trace_body_decode_permit(started.elapsed(), capacity_before);
        Ok(permit)
    }

    /// Send `body` through its sequencer slot, then settle `funding`. No
    /// suspension point separates them. Funding releases only after the body
    /// counts in `sequencer_input_bytes`, so the bytes are never invisible to both
    /// the limiter and the resident snapshot.
    pub(super) fn send_under_receipt(
        &self,
        body: ReceivedBody<Option<AdmittedBody>>,
        owner: zakura_header_chain::BodyWorkOwner,
        funding: ByteReservation,
        admitted_estimate_bytes: Option<u64>,
    ) {
        let settlement = ReceiptSettlement::new(self, owner, body.height, funding);
        self.forward_body_to_sequencer(body, owner, admitted_estimate_bytes);
        drop(settlement);
    }

    /// Adopt the parser's exclusive payloads before retaining a decoded body.
    /// Refusal keeps only the raw frame for a later admission attempt.
    ///
    /// The input-queue room check comes first. A pool admission that the check
    /// then refused would release its charge at once and wake every capacity waiter.
    pub(super) fn admit_decoded_body(
        &self,
        body: ReceivedBody,
    ) -> Result<(ReceivedBody<Option<AdmittedBody>>, u64), AdmissionFailure> {
        let ReceivedBody {
            height,
            hash,
            previous_block_hash,
            block,
            raw_payload,
            serialized_bytes,
            permit,
        } = body;
        let (block, decoded_bytes) = self.admit_block(block)?;
        Ok((
            ReceivedBody {
                height,
                hash,
                previous_block_hash,
                block,
                raw_payload,
                serialized_bytes,
                permit,
            },
            decoded_bytes,
        ))
    }

    fn admit_block(
        &self,
        block: Arc<block::Block>,
    ) -> Result<(Option<AdmittedBody>, u64), AdmissionFailure> {
        if self.decoded_pool.is_closed() {
            self.cancel.cancel();
            return Err(AdmissionFailure::Closed);
        }
        let block = match Arc::try_unwrap(block) {
            Ok(block) => block,
            Err(block) => return Ok((None, block.attributed_memory_size_bytes())),
        };
        let block = MeasuredBlock::new(block);
        let bytes = block.bytes();
        let Some(charge) = charge_decoded_handoff(
            &self.sequencer_input_decoded_attributed_memory_bytes,
            &self.sequencer_view,
            bytes,
        ) else {
            return Ok((None, bytes));
        };
        match self
            .decoded_pool
            .try_admit_measured(block, Admission::WithinLimit)
        {
            Ok(block) => Ok((Some(AdmittedBody { block, charge }), bytes)),
            Err(rejected) => {
                if !matches!(
                    rejected.reason,
                    AdmissionFailure::Capacity | AdmissionFailure::AliasedPayload
                ) {
                    self.decoded_pool.close();
                    self.cancel.cancel();
                    return Err(rejected.reason);
                }
                Ok((None, rejected.decoded_bytes))
            }
        }
    }

    fn forward_body_to_sequencer(
        &self,
        received: ReceivedBody<Option<AdmittedBody>>,
        owner: zakura_header_chain::BodyWorkOwner,
        admitted_estimate_bytes: Option<u64>,
    ) {
        let ReceivedBody {
            height,
            hash,
            previous_block_hash,
            block,
            raw_payload,
            serialized_bytes,
            permit,
        } = received;
        let received_at = Instant::now();
        let sequencer_send_started = time::Instant::now();
        // The body owns the decoded charge from here, with no await between.
        let body = match block {
            Some(AdmittedBody { block, charge }) => {
                charge.hand_over();
                BufferedBlockBody::for_handoff(block, raw_payload)
            }
            None => BufferedBlockBody::RawFramePayload(raw_payload),
        };
        let body = SequencedBody::new_handed_off(
            owner,
            self.source,
            height,
            hash,
            previous_block_hash,
            body,
            serialized_bytes,
            admitted_estimate_bytes,
            self.peer.clone(),
            received_at,
            self.sequencer_input_bytes.clone(),
            self.sequencer_input_decoded_attributed_memory_bytes.clone(),
        );

        // Report retention before sending because the sequencer can dequeue
        // the body immediately on another thread.
        #[cfg(any(test, zakura_sim))]
        {
            use super::super::block_sync_observe::{BlockEvent, HeldBody, Holder, HolderChange};
            observe!(
                self.observer,
                BlockEvent::Holder {
                    body: HeldBody {
                        height,
                        owner,
                        bytes: serialized_bytes,
                    },
                    holder: Holder::Queued {
                        allocation: body.body.allocation(),
                        decoded_attributed_memory_bytes: body
                            .body
                            .measured_decoded_attributed_memory_bytes(),
                    },
                    change: HolderChange::Retained,
                }
            );
        }
        permit.send(body);
        self.trace_body_sequencer_sent(height, sequencer_send_started.elapsed());
    }

    /// Reports that the routine is about to send the body at `height` to the
    /// sequencer while its request still charges `charge` to the budget. The
    /// report comes before the send, because the sequencer can dequeue the
    /// body as soon as the send succeeds. Reports nothing when `charge` is 0,
    /// because then the budget never counted the body.
    /// The receipt owner reports the end after the send or during unwind.
    #[cfg(any(test, zakura_sim))]
    fn observe_handoff_started(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
        charge: u64,
    ) {
        if charge == 0 {
            return;
        }
        observe!(
            self.observer,
            super::super::block_sync_observe::BlockEvent::HandoffStarted {
                peer: self.peer.clone(),
                owner,
                height,
                charge,
            }
        );
    }
}

/// A body the decoded pool admitted, with its input-queue charge.
pub(super) struct AdmittedBody {
    block: OwnedBlock,
    charge: DecodedHandoffCharge,
}

/// A body decoded from an authorized frame, with the sequencer slot acquired
/// before decode. Holding the slot means nothing on the receipt path can wait.
///
/// `B` is the decoded block: the parser's, then what decoded admission kept.
pub(super) struct ReceivedBody<B = Arc<block::Block>> {
    pub(super) height: block::Height,
    pub(super) hash: block::Hash,
    pub(super) previous_block_hash: block::Hash,
    pub(super) block: B,
    /// The frame payload `block` was decoded from.
    pub(super) raw_payload: Arc<[u8]>,
    /// The length of `raw_payload`, measured on the per-peer task.
    pub(super) serialized_bytes: u64,
    pub(super) permit: mpsc::OwnedPermit<SequencedBody>,
}

/// Keeps a receipt charge live until the synchronous send finishes or unwinds.
/// Other peers can serve the floor while this peer transfers and settles its body.
pub(super) struct ReceiptSettlement<'a> {
    /// Drops after [`Drop::drop`] releases the funding, so the routine stays
    /// marked through the release.
    _blocked: HandoffBlocked<'a>,
    funding: Option<ByteReservation>,
    #[cfg(any(test, zakura_sim))]
    handoff: HandoffReport,
}

/// What a [`ReceiptSettlement`] reports when its handoff ends.
#[cfg(any(test, zakura_sim))]
struct HandoffReport {
    observer: super::super::block_sync_observe::ObserverSlot,
    peer: ZakuraPeerId,
    owner: zakura_header_chain::BodyWorkOwner,
    height: block::Height,
}

impl<'a> ReceiptSettlement<'a> {
    pub(super) fn new(
        io: &'a PeerIO,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
        funding: ByteReservation,
    ) -> Self {
        // A concurrent ranker must skip this peer if the transfer is preempted.
        // The synchronous transfer does not need to wake parked rankers.
        let blocked = HandoffBlocked::set(&io.handoff_blocked);
        #[cfg(any(test, zakura_sim))]
        io.observe_handoff_started(owner, height, funding.bytes());
        #[cfg(not(any(test, zakura_sim)))]
        let _ = (owner, height);
        Self {
            _blocked: blocked,
            funding: Some(funding),
            #[cfg(any(test, zakura_sim))]
            handoff: HandoffReport {
                observer: io.observer.clone(),
                peer: io.peer.clone(),
                owner,
                height,
            },
        }
    }
}

impl Drop for ReceiptSettlement<'_> {
    fn drop(&mut self) {
        let funding = self
            .funding
            .take()
            .expect("receipt owns its funding until drop");
        #[cfg(not(any(test, zakura_sim)))]
        drop(funding);
        #[cfg(any(test, zakura_sim))]
        {
            let charge = funding.bytes();
            let handoff = &self.handoff;
            funding.release_then(|| {
                observe!(
                    handoff.observer,
                    super::super::block_sync_observe::BlockEvent::HandoffEnded {
                        peer: handoff.peer.clone(),
                        owner: handoff.owner,
                        height: handoff.height,
                        charge,
                    }
                );
            });
        }
    }
}

/// Marks a routine blocked on its sequencer handoff, from creation until
/// drop. Other routines skip a blocked routine as a floor carrier
/// ([`PeerRegistry::floor_has_preferred_unsaturated_server`]), because it
/// can't request the floor until its handoff completes. Dropping the guard,
/// including by cancelling the wait, clears the flag.
struct HandoffBlocked<'a>(&'a AtomicBool);

impl<'a> HandoffBlocked<'a> {
    /// Sets `flag` without waking anyone, for a handoff that does not wait.
    fn set(flag: &'a AtomicBool) -> Self {
        flag.store(true, Ordering::Relaxed);
        HandoffBlocked(flag)
    }

    /// Sets the routine's flag before a handoff waits. A routine that published
    /// a free slot may be some other routine's preferred floor carrier, so this
    /// wakes them to rank the floor again.
    fn start(io: &'a PeerIO, published_free_slots: bool) -> Self {
        let blocked = Self::set(&io.handoff_blocked);
        if published_free_slots {
            io.registry.notify_floor_ranking_changed();
        }
        blocked
    }
}

impl Drop for HandoffBlocked<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

/// Live write handles of this session's outstanding requests, under the keys
/// of the outstanding records in [`PeerState`](super::PeerState).
///
/// Transport changes each handle concurrently. Reading a handle observes the
/// write; settling it arbitrates a queued frame against writer startup.
#[derive(Debug, Default)]
pub(super) struct RequestWrites {
    statuses: BTreeMap<RequestKey, RequestWriteStatus>,
    /// Requests whose frame write had not started when last checked. Transport
    /// can skip only these requests.
    unstarted: BTreeSet<RequestKey>,
}

impl RequestWrites {
    /// Track the write of the request just recorded under `key`.
    pub(super) fn insert(&mut self, key: RequestKey, status: RequestWriteStatus) {
        self.unstarted.insert(key);
        self.statuses.insert(key, status);
    }

    /// Where the write of `key` stands now.
    #[cfg(any(test, zakura_sim))]
    pub(super) fn phase(
        &self,
        key: RequestKey,
    ) -> Option<super::super::work_queue::RequestWritePhase> {
        self.statuses.get(&key).map(RequestWriteStatus::phase)
    }

    /// Requests whose frame transport skipped, in issue order. Forget requests
    /// whose write has started, because transport can no longer skip them.
    pub(super) fn skipped(&mut self) -> Vec<RequestKey> {
        let statuses = &self.statuses;
        let mut skipped = Vec::new();
        self.unstarted.retain(|key| {
            let status = &statuses[key];
            if status.was_skipped() {
                skipped.push(*key);
                true
            } else {
                !status.has_started()
            }
        });
        skipped
    }

    /// Settle the write of `key` and stop tracking it. A queued frame expires
    /// here unless transport has already started writing it.
    pub(super) fn settle(&mut self, key: RequestKey) -> WriteOutcome {
        self.unstarted.remove(&key);
        let status = self
            .statuses
            .remove(&key)
            .expect("every outstanding request keeps its write handle until it settles");
        if status.expire_unwritten() || status.was_skipped() {
            WriteOutcome::Skipped
        } else {
            WriteOutcome::Started
        }
    }
}
