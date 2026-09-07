//! Resource admission for serving inbound `GetBlocks` requests.
//!
//! This module turns the generic regulation primitives into one message policy.
//! A decoded request first owns bounded pending state. Its admission task then
//! acquires a response producer before the state query starts. The query, result,
//! and queued frames share that producer until the last owner drops. A blocked
//! transport writer therefore prevents another query for the same session.

use std::sync::Arc;

#[cfg(test)]
use super::wire::MAX_BS_BLOCKS_PER_REQUEST;
use super::{config::*, *};
use crate::zakura::{
    regulation::{
        AcquiredWorkSlot, RequestAdmission, RequestSession, ResponsePermit, SlotBudget, SlotPermit,
        WorkAttempt, WorkBlocked, WorkBound, WorkLease,
    },
    transport::FrameGuard,
};

mod policy;
use policy::{GetBlocksPolicy, GetBlocksRequest};

/// The bounded work declaration for one decoded request.
#[cfg(test)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct GetBlocksServingCost {
    /// Count after applying this node's advertised response-count cap.
    pub(super) count: u32,
    /// Worst-case encoded response payload owned until settlement or transport handoff.
    pub(super) response_cap: u64,
}

/// Compute the worst-case work a valid request can cause using checked arithmetic.
#[cfg(test)]
pub(super) fn serving_cost(
    config: &ZakuraBlockSyncConfig,
    requested_count: u32,
) -> Result<GetBlocksServingCost, &'static str> {
    Ok(GetBlocksServingCost {
        count: requested_count.min(inbound_get_blocks_count_limit(config)),
        response_cap: GetBlocksPolicy::new(config).response_cap_for_count(requested_count)?,
    })
}

/// Validate that every legal request can eventually fit every configured bound.
pub(super) fn validate_config(config: &ZakuraBlockSyncConfig) -> Result<(), &'static str> {
    if u64::from(config.max_response_bytes) < block::MAX_BLOCK_BYTES {
        return Err("max_response_bytes must cover one maximum-size block");
    }
    let regulation = &config.get_blocks_regulation;
    if regulation.node_active_requests == 0 {
        return Err("get_blocks_regulation.node_active_requests must be greater than zero");
    }
    if regulation.node_active_requests > tokio::sync::Semaphore::MAX_PERMITS {
        return Err("get_blocks_regulation.node_active_requests exceeds Tokio's semaphore limit");
    }
    if regulation.peer_pending_requests == 0 {
        return Err("get_blocks_regulation.peer_pending_requests must be greater than zero");
    }
    if regulation.peer_pending_requests > tokio::sync::Semaphore::MAX_PERMITS {
        return Err("get_blocks_regulation.peer_pending_requests exceeds Tokio's semaphore limit");
    }
    if regulation.node_pending_requests == 0 {
        return Err("get_blocks_regulation.node_pending_requests must be greater than zero");
    }
    if regulation.node_pending_requests > tokio::sync::Semaphore::MAX_PERMITS {
        return Err("get_blocks_regulation.node_pending_requests exceeds Tokio's semaphore limit");
    }
    if regulation.query_timeout < Duration::from_millis(1) {
        return Err("get_blocks_regulation.query_timeout must be at least 1ms");
    }

    if regulation.node_pending_requests < regulation.peer_pending_requests {
        return Err("get_blocks_regulation.node_pending_requests must cover one session queue");
    }

    Ok(())
}

/// Pending requests retained by one stream while its oldest request waits for work.
pub(super) fn pending_input_capacity_per_session(config: &ZakuraBlockSyncConfig) -> usize {
    config.get_blocks_regulation.peer_pending_requests
}

/// Node-owned GetBlocks resources shared by every peer routine.
#[derive(Clone, Debug)]
pub(super) struct GetBlocksServingRegulator {
    inner: Arc<RegulatorInner>,
}

#[derive(Debug)]
struct RegulatorInner {
    admission: RequestAdmission<GetBlocksPolicy>,
    #[cfg(test)]
    node_active: SlotBudget,
    node_pending: SlotBudget,
    session_pending_capacity: usize,
    #[cfg(test)]
    sessions: StdMutex<Vec<(SlotBudget, SlotBudget)>>,
}

#[derive(Debug)]
struct SessionResources {
    pending: SlotBudget,
    work: RequestSession<GetBlocksPolicy>,
    #[cfg(test)]
    active: SlotBudget,
}

impl GetBlocksServingRegulator {
    /// Create the GetBlocks node policy from validated block-sync configuration.
    pub(super) fn new(config: ZakuraBlockSyncConfig) -> Self {
        debug_assert!(validate_config(&config).is_ok());
        let regulation = &config.get_blocks_regulation;
        let session_pending_capacity = pending_input_capacity_per_session(&config);
        let node_active = SlotBudget::new(regulation.node_active_requests)
            .expect("GetBlocks configuration validates the active-request capacity");
        Self {
            inner: Arc::new(RegulatorInner {
                admission: RequestAdmission::new(
                    GetBlocksPolicy::new(&config),
                    node_active.clone(),
                    GetBlocksPolicy::SESSION_PRODUCERS,
                ),
                #[cfg(test)]
                node_active,
                node_pending: SlotBudget::new(regulation.node_pending_requests)
                    .expect("GetBlocks configuration validates the pending-request capacity"),
                session_pending_capacity,
                #[cfg(test)]
                sessions: StdMutex::new(Vec::new()),
            }),
        }
    }

    /// Create one session policy within the node admission bounds.
    pub(super) fn session(&self, peer: ZakuraPeerId, session_id: u64) -> GetBlocksServingSession {
        let work = self.inner.admission.session();
        let resources = Arc::new(SessionResources {
            pending: SlotBudget::new(self.inner.session_pending_capacity)
                .expect("GetBlocks configuration validates the pending-request capacity"),
            #[cfg(test)]
            active: work.session_budget().clone(),
            work,
        });
        #[cfg(test)]
        self.inner
            .sessions
            .lock()
            .expect("GetBlocks session-resource mutex should not be poisoned")
            .push((resources.active.clone(), resources.pending.clone()));

        GetBlocksServingSession {
            regulator: self.clone(),
            peer,
            session_id,
            resources,
        }
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> ServingRegulationSnapshot {
        let sessions = self
            .inner
            .sessions
            .lock()
            .expect("GetBlocks session-resource mutex should not be poisoned");
        let mut session_active = 0usize;
        let mut session_pending = 0usize;
        for (active, pending) in sessions.iter() {
            session_active += active.reserved();
            session_pending = session_pending.saturating_add(pending.reserved());
        }
        ServingRegulationSnapshot {
            node_active: self.inner.node_active.reserved(),
            node_pending: self.inner.node_pending.reserved(),
            session_active,
            session_pending,
        }
    }
}

/// Per-session entry point for pending ownership and work admission.
#[derive(Clone, Debug)]
pub(super) struct GetBlocksServingSession {
    regulator: GetBlocksServingRegulator,
    peer: ZakuraPeerId,
    session_id: u64,
    resources: Arc<SessionResources>,
}

impl GetBlocksServingSession {
    /// Reserve bounded memory for one decoded request before retaining it.
    pub(super) fn try_retain_input(
        &self,
        start_height: block::Height,
        count: u32,
    ) -> Result<PendingGetBlocksRequest, PendingInputBlocked> {
        let session = self
            .resources
            .pending
            .try_reserve()
            .ok_or_else(PendingInputBlocked::session)?;
        let node = self
            .regulator
            .inner
            .node_pending
            .try_reserve()
            .ok_or_else(PendingInputBlocked::node)?;
        Ok(PendingGetBlocksRequest {
            start_height,
            count,
            _session: session,
            _node: node,
            _resources: self.resources.clone(),
        })
    }

    /// Wait for pending ownership while the routine continues processing completions.
    pub(super) async fn retain_input(
        &self,
        start_height: block::Height,
        count: u32,
    ) -> PendingGetBlocksRequest {
        let session = self.resources.pending.reserve().await;
        let node = self.regulator.inner.node_pending.reserve().await;
        PendingGetBlocksRequest {
            start_height,
            count,
            _session: session,
            _node: node,
            _resources: self.resources.clone(),
        }
    }

    /// Apply the declared codec before retaining or admitting an inbound request.
    pub(super) fn decode_request(
        &self,
        frame: Frame,
    ) -> Result<BlockSyncMessage, BlockSyncWireError> {
        self.resources
            .work
            .decode(frame)
            .map(|request| BlockSyncMessage::GetBlocks {
                start_height: request.start_height,
                count: request.count,
            })
    }

    /// Admit an already decoded, retained request before dispatching its state work.
    pub(super) fn try_admit_request(
        &self,
        request: &PendingGetBlocksRequest,
        acquired: Option<AcquiredAdmissionSlot>,
    ) -> Result<AdmissionAttempt, AdmissionBlocked> {
        self.admit(
            &GetBlocksRequest {
                start_height: request.start_height,
                count: request.count,
            },
            acquired,
        )
    }

    fn admit(
        &self,
        request: &GetBlocksRequest,
        acquired: Option<AcquiredAdmissionSlot>,
    ) -> Result<AdmissionAttempt, AdmissionBlocked> {
        let work = self
            .resources
            .work
            .try_admit(request, acquired)
            .map_err(AdmissionBlocked)?;
        Ok(AdmissionAttempt {
            peer: self.peer.clone(),
            session_id: self.session_id,
            work,
        })
    }

    #[cfg(any(test, feature = "zakura-testkit"))]
    pub(super) fn try_admit(&self, count: u32) -> Result<AdmissionAttempt, AdmissionBlocked> {
        self.admit(
            &GetBlocksRequest {
                start_height: block::Height(0),
                count,
            },
            None,
        )
    }

    #[cfg(test)]
    pub(super) fn try_admit_with_slot(
        &self,
        count: u32,
        acquired: Option<AcquiredAdmissionSlot>,
    ) -> Result<AdmissionAttempt, AdmissionBlocked> {
        self.admit(
            &GetBlocksRequest {
                start_height: block::Height(0),
                count,
            },
            acquired,
        )
    }
}

pub(super) type AcquiredAdmissionSlot = AcquiredWorkSlot;

/// The pending bound currently delaying one decoded request.
#[derive(Clone, Debug)]
pub(super) struct PendingInputBlocked {
    kind: PendingBoundKind,
}

/// Scope of a retained-request capacity delay.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum PendingBoundKind {
    /// This session has retained its advertised request window.
    Session,
    /// All sessions together have reached the node's decoded-request bound.
    Node,
}

impl PendingInputBlocked {
    fn session() -> Self {
        Self {
            kind: PendingBoundKind::Session,
        }
    }

    fn node() -> Self {
        Self {
            kind: PendingBoundKind::Node,
        }
    }

    /// Stable low-cardinality label for metrics and traces.
    pub(super) fn label(&self) -> &'static str {
        match self.kind {
            PendingBoundKind::Session => "session_pending",
            PendingBoundKind::Node => "node_pending",
        }
    }
}

/// One decoded request plus the memory slots that permit retaining it.
#[derive(Debug)]
#[must_use = "a retained GetBlocks request must be forwarded or explicitly dropped"]
pub(super) struct PendingGetBlocksRequest {
    start_height: block::Height,
    count: u32,
    _session: SlotPermit,
    _node: SlotPermit,
    _resources: Arc<SessionResources>,
}

impl PendingGetBlocksRequest {
    /// End pending ownership and return the validated request fields.
    pub(super) fn into_parts(self) -> (block::Height, u32) {
        (self.start_height, self.count)
    }
}

/// The work bound that rejected an otherwise valid request.
#[derive(Debug)]
pub(super) struct AdmissionBlocked(WorkBlocked);

/// Stable resource names used by low-cardinality delay observations.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum BoundKind {
    PeerActive,
    NodeActive,
}

impl BoundKind {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::PeerActive => "peer_active",
            Self::NodeActive => "node_active",
        }
    }
}

impl AdmissionBlocked {
    pub(super) fn kind(&self) -> BoundKind {
        match self.0.kind() {
            WorkBound::Session => BoundKind::PeerActive,
            WorkBound::Node => BoundKind::NodeActive,
        }
    }

    pub(super) async fn wait(self) -> Option<AcquiredAdmissionSlot> {
        Some(self.0.wait().await)
    }
}

/// Provisional ownership of every resource needed before state work starts.
#[derive(Debug)]
#[must_use = "dropping a GetBlocks admission attempt rolls back every reservation"]
pub(super) struct AdmissionAttempt {
    peer: ZakuraPeerId,
    session_id: u64,
    work: WorkAttempt,
}

impl AdmissionAttempt {
    pub(super) fn peer(&self) -> &ZakuraPeerId {
        &self.peer
    }

    pub(super) fn session_id(&self) -> u64 {
        self.session_id
    }

    /// Transfer admitted resources to the reactor ledger for this exact session.
    pub(super) fn commit(self) -> GetBlocksServingPermit {
        metrics::counter!("sync.block.serving.admitted").increment(1);
        GetBlocksServingPermit {
            response: self.work.commit(),
        }
    }
}

/// Committed request ownership retained by the reactor's serving ledger.
#[derive(Debug)]
#[must_use = "the serving ledger must retain this permit until request settlement"]
pub(super) struct GetBlocksServingPermit {
    response: ResponsePermit,
}

impl GetBlocksServingPermit {
    pub(super) fn can_queue_frame(&self, bytes: u64) -> bool {
        self.response.can_queue_frame(bytes)
    }

    pub(super) fn frame_guard(&mut self, bytes: u64) -> FrameGuard {
        self.response.frame_guard(bytes)
    }

    pub(super) fn query_lease(&self) -> BlockRangeQueryLease {
        BlockRangeQueryLease {
            work: self.response.work_lease(),
        }
    }
}

/// Capacity retained by a serving query and its completed response.
///
/// The driver must claim a query once, retain this lease until its underlying
/// state future completes (even after a response timeout), and transfer it to
/// `BlockRangeResponseReady` with the returned blocks. Clones share the same
/// charge and cannot start additional queries. Ledger removal cancels delivery,
/// but resources return only after the last ledger, worker, and result owner drops.
#[derive(Clone, Debug)]
pub struct BlockRangeQueryLease {
    work: WorkLease,
}

impl BlockRangeQueryLease {
    /// Claim the only execution, serialized against ledger closure.
    ///
    /// If closure wins, no read starts. If the claim wins, the worker retains
    /// capacity until the read finishes, even if delivery is then cancelled.
    pub fn try_start(&self) -> bool {
        self.work.try_start()
    }

    /// Whether the request no longer has a live delivery owner.
    pub fn is_cancelled(&self) -> bool {
        self.work.is_cancelled()
    }

    /// Wait for the ledger to close. This does not cancel an active state read.
    pub async fn cancelled(&self) {
        self.work.cancelled().await;
    }
}

#[cfg(any(test, feature = "zakura-testkit"))]
pub(crate) fn query_lease_for_test() -> BlockRangeQueryLease {
    let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
    let session = regulator.session(
        ZakuraPeerId::new(vec![0; 32]).expect("a 32-byte test identity fits"),
        0,
    );
    let permit = session
        .try_admit(1)
        .expect("the test budget is initially full")
        .commit();
    let mut lease = permit.query_lease();
    // Standalone driver fixtures have no reactor ledger to signal cancellation.
    lease.work.detach_cancellation_for_test();
    lease
}

#[cfg(test)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct ServingRegulationSnapshot {
    pub(super) node_active: usize,
    pub(super) node_pending: usize,
    pub(super) session_active: usize,
    pub(super) session_pending: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(byte: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![byte; 32]).expect("test peer id is within bounds")
    }

    #[test]
    fn query_and_result_keep_capacity_after_the_ledger_closes() {
        let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
        let session = regulator.session(peer(8), 8);
        let permit = session
            .try_admit(1)
            .expect("the initial request fits")
            .commit();
        let query = permit.query_lease();
        assert!(query.try_start());
        assert!(
            !query.clone().try_start(),
            "cloning a query never authorizes another read"
        );
        let result = query.clone();

        drop(permit);
        assert!(query.is_cancelled());
        assert_eq!(regulator.snapshot().node_active, 1);

        drop(query);

        drop(result);
        assert_eq!(regulator.snapshot().node_active, 0);
    }

    #[test]
    fn closed_ledger_prevents_queued_query_execution() {
        let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
        let session = regulator.session(peer(9), 9);
        let permit = session
            .try_admit(1)
            .expect("the initial request fits")
            .commit();
        let query = permit.query_lease();
        drop(permit);
        assert!(!query.try_start());
        drop(query);
        assert_eq!(regulator.snapshot().node_active, 0);
    }

    #[test]
    fn separately_issued_query_leases_share_one_execution_claim() {
        let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
        let session = regulator.session(peer(9), 9);
        let permit = session.try_admit(1).unwrap().commit();
        let first = permit.query_lease();
        let second = permit.query_lease();
        assert!(first.try_start());
        assert!(!second.try_start());
        drop(permit);
        assert!(first.is_cancelled());
        assert!(second.is_cancelled());
        assert_eq!(regulator.snapshot().node_active, 1);
        drop((first, second));
        assert_eq!(regulator.snapshot().node_active, 0);
    }

    #[test]
    fn concurrent_claims_and_cancellation_preserve_one_charged_owner() {
        use std::{sync::Barrier, thread};

        // Exercise overlapping calls; deterministic tests above require both
        // ordered outcomes. This does not claim exhaustive schedule coverage.
        for _ in 0..64 {
            let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
            let session = regulator.session(peer(9), 9);
            let permit = session.try_admit(1).unwrap().commit();
            let lease = permit.query_lease();

            let barrier = Barrier::new(4);

            thread::scope(|scope| {
                let first = scope.spawn(|| {
                    barrier.wait();
                    lease.try_start()
                });
                let second = scope.spawn(|| {
                    barrier.wait();
                    lease.try_start()
                });
                let cancellation = scope.spawn(|| {
                    barrier.wait();
                    drop(permit);
                });
                barrier.wait();
                let claims =
                    usize::from(first.join().unwrap()) + usize::from(second.join().unwrap());
                cancellation.join().unwrap();
                assert!(claims <= 1);
            });

            assert!(lease.is_cancelled());
            assert!(
                !lease.try_start(),
                "ledger closure permanently prevents new claims"
            );
            assert_eq!(regulator.snapshot().node_active, 1);

            drop(lease);
            assert_eq!(regulator.snapshot().node_active, 0);
        }
    }

    #[tokio::test]
    async fn admission_consumes_the_slot_delivered_to_its_waiter() {
        use futures::poll;

        let mut config = ZakuraBlockSyncConfig::default();
        config.get_blocks_regulation.node_active_requests = 1;
        let regulator = GetBlocksServingRegulator::new(config);
        let session = regulator.session(peer(10), 10);
        let owner = session.try_admit(1).expect("one request fits").commit();
        let waiting_session = regulator.session(peer(11), 11);
        let blocked = waiting_session
            .try_admit(1)
            .expect_err("the active slot is owned");
        assert_eq!(blocked.kind(), BoundKind::NodeActive);
        let wait = blocked.wait();
        tokio::pin!(wait);
        assert!(poll!(&mut wait).is_pending());
        drop(owner);
        let acquired = tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("released capacity reaches its waiter");
        let admitted = waiting_session
            .try_admit_with_slot(1, acquired)
            .expect("the retry retains its assigned slot")
            .commit();
        assert_eq!(regulator.snapshot().node_active, 1);
        drop(admitted);
        assert_eq!(regulator.snapshot().node_active, 0);
    }

    /// A fixed application-path measurement, intentionally outside the fast test lane.
    /// It uses real serialized blocks and a promptly draining in-memory transport.
    #[tokio::test]
    #[ignore = "local measurement; run explicitly with --ignored --nocapture"]
    #[allow(clippy::print_stderr)] // explicit measurement output for the local operator
    async fn serving_fixed_workload_measurement() {
        let vectors = &*zakura_test::vectors::MAINNET_BLOCKS;
        let smallest = vectors
            .iter()
            .filter(|(height, _)| **height > 0)
            .min_by_key(|(_, bytes)| bytes.len())
            .unwrap();
        let largest = vectors.iter().max_by_key(|(_, bytes)| bytes.len()).unwrap();
        let mut cases = Vec::new();
        for (label, (height, bytes)) in [("small", smallest), ("large", largest)] {
            cases.push((
                label,
                block::Height(*height),
                Arc::new(
                    block::Block::zcash_deserialize(*bytes)
                        .expect("committed block fixture decodes"),
                ),
                bytes.len(),
            ));
        }
        // Reuse the existing fixed-shape serialization fixture. It is not a
        // consensus-validation fixture and contributes no generated event histories.
        let corpus = crate::zakura::testkit::SyntheticBlockCorpus::generate(
            1,
            1,
            crate::zakura::testkit::SyntheticBlockShape {
                target_block_bytes: Some(1_999_000),
            },
        );
        cases.push((
            "near_limit",
            block::Height(1),
            corpus.block_at(block::Height(1)).unwrap(),
            corpus.size_at(block::Height(1)).unwrap(),
        ));
        for (label, height, body, body_bytes) in cases {
            for peers in [1u8, 4] {
                for regulated in [false, true] {
                    let regulator =
                        GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
                    let mut sessions = Vec::new();
                    for index in 0..peers {
                        let identity = peer(index);
                        let (send, recv) = crate::zakura::transport::framed_channel(2);
                        sessions.push((
                            regulator.session(identity.clone(), u64::from(index)),
                            BlockSyncPeerSession::for_test(
                                identity,
                                send,
                                CancellationToken::new(),
                            ),
                            recv,
                        ));
                    }
                    let started = Instant::now();
                    for _ in 0..32 {
                        for (policy, session, receiver) in &mut sessions {
                            if regulated {
                                let mut acquired = None;
                                let attempt = loop {
                                    match policy.try_admit_with_slot(1, acquired.take()) {
                                        Ok(attempt) => break attempt,
                                        Err(blocked) => acquired = blocked.wait().await,
                                    }
                                };
                                let mut permit = attempt.commit();
                                session
                                    .try_send_regulated_block(body.clone(), &mut permit)
                                    .expect("the reader drained its previous response");
                                session
                                    .try_send_regulated_blocks_done(height, 1, &mut permit)
                                    .expect("the terminal frame fits");
                            } else {
                                session
                                    .try_send_block(body.clone())
                                    .expect("the reader drained its previous response");
                                session
                                    .try_send_blocks_done(height, 1)
                                    .expect("the terminal frame fits");
                            }
                            let received = receiver.recv().await.expect("the block was queued");
                            assert_eq!(received.payload.len(), body_bytes + 1);
                            assert!(receiver.recv().await.is_some());
                        }
                    }
                    let elapsed = started.elapsed();

                    assert_eq!(regulator.snapshot().node_active, 0);
                    eprintln!("serving_measurement block={label} body_bytes={} peers={peers} responses={} regulated={regulated} elapsed_ms={:.3}", body_bytes, u32::from(peers) * 32, elapsed.as_secs_f64() * 1000.0);
                }
            }
        }
    }

    #[test]
    fn cost_includes_block_discriminators_and_terminal() {
        let mut config = ZakuraBlockSyncConfig {
            max_blocks_per_response: 2,
            max_response_bytes: u32::try_from(block::MAX_BLOCK_BYTES * 2)
                .expect("two maximum block bodies fit u32"),
            ..ZakuraBlockSyncConfig::default()
        };

        let cost = serving_cost(&config, 2).expect("the default bounds do not overflow");
        assert_eq!(cost.count, 2);
        assert_eq!(
            cost.response_cap,
            block::MAX_BLOCK_BYTES * 2 + 2 + GET_BLOCKS_TERMINAL_PAYLOAD_BYTES
        );

        config.max_blocks_per_response = 3;
        config.max_response_bytes = u32::try_from(block::MAX_BLOCK_BYTES).unwrap();
        let byte_limited = serving_cost(&config, MAX_BS_BLOCKS_PER_REQUEST)
            .expect("the byte-limited cost is representable");
        assert_eq!(byte_limited.count, 3);
        assert_eq!(
            byte_limited.response_cap,
            GET_BLOCKS_TERMINAL_PAYLOAD_BYTES + 3 + block::MAX_BLOCK_BYTES,
            "the body-byte cap is separate from discriminators and the terminal frame",
        );
    }

    #[test]
    fn config_rejects_nonprogressing_or_unbounded_admission_settings() {
        let base = ZakuraBlockSyncConfig::default();

        let mut no_active_slots = base.clone();
        no_active_slots.get_blocks_regulation.node_active_requests = 0;
        assert_eq!(
            validate_config(&no_active_slots),
            Err("get_blocks_regulation.node_active_requests must be greater than zero"),
        );

        let mut no_pending_slots = base.clone();
        no_pending_slots.get_blocks_regulation.node_pending_requests = 0;
        assert_eq!(
            validate_config(&no_pending_slots),
            Err("get_blocks_regulation.node_pending_requests must be greater than zero"),
        );

        let mut no_query_time = base;
        no_query_time.get_blocks_regulation.query_timeout = Duration::ZERO;
        assert_eq!(
            validate_config(&no_query_time),
            Err("get_blocks_regulation.query_timeout must be at least 1ms"),
        );
    }

    #[tokio::test(start_paused = true)]
    async fn provisional_admission_rolls_back_every_earlier_reservation() {
        let config = ZakuraBlockSyncConfig::default();
        let regulator = GetBlocksServingRegulator::new(config);
        let session = regulator.session(peer(1), 1);
        let other_peer = regulator.session(peer(7), 1);
        let first = session.try_admit(1).expect("the first request fits");
        let before = regulator.snapshot();
        let blocked = session
            .try_admit(1)
            .expect_err("the peer producer is occupied by the first request");
        assert_eq!(blocked.kind(), BoundKind::PeerActive);
        assert_eq!(regulator.snapshot(), before);

        let independent = other_peer
            .try_admit(1)
            .expect("one peer's producer does not consume another peer's capacity");
        assert_eq!(regulator.snapshot().node_active, 2);
        drop(independent);
        drop(first);
    }

    #[tokio::test(start_paused = true)]
    async fn completed_requests_release_capacity_without_waiting_for_time() {
        let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
        let session = regulator.session(peer(2), 2);
        let now = time::Instant::now();
        for _ in 0..4096 {
            let mut permit = session
                .try_admit(1)
                .expect("released capacity admits work")
                .commit();
            let frame = permit.frame_guard(GET_BLOCKS_TERMINAL_PAYLOAD_BYTES);
            drop(permit);
            assert_eq!(regulator.snapshot().node_active, 1);
            drop(frame);

            assert_eq!(regulator.snapshot().node_active, 0);
        }
        assert_eq!(
            time::Instant::now(),
            now,
            "admission has no bandwidth refill timer"
        );
    }

    #[test]
    fn frames_keep_the_producer_until_the_last_write_finishes() {
        let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
        let session = regulator.session(peer(3), 3);
        let other = regulator.session(peer(4), 4);
        let mut permit = session.try_admit(1).unwrap().commit();
        let block = permit.frame_guard(100);
        let terminal = permit.frame_guard(9);
        drop(permit);
        assert_eq!(
            session.try_admit(1).unwrap_err().kind(),
            BoundKind::PeerActive
        );
        assert!(other.try_admit(1).is_ok());
        drop(block);
        assert!(session.try_admit(1).is_err());
        drop(terminal);
        assert_eq!(regulator.snapshot().node_active, 0);
        assert!(session.try_admit(1).is_ok());
    }

    #[test]
    fn pending_requests_are_bounded_per_session_and_node() {
        let mut config = ZakuraBlockSyncConfig::default();
        config.get_blocks_regulation.peer_pending_requests = 1;
        config.get_blocks_regulation.node_pending_requests = 2;
        let regulator = GetBlocksServingRegulator::new(config);
        let first_session = regulator.session(peer(4), 4);
        let second_session = regulator.session(peer(5), 5);
        let third_session = regulator.session(peer(6), 6);

        let first = first_session
            .try_retain_input(block::Height(1), 1)
            .expect("the first request fits");
        let second = second_session
            .try_retain_input(block::Height(2), 1)
            .expect("the second request fits the node");
        assert_eq!(regulator.snapshot().node_pending, 2);
        assert_eq!(regulator.snapshot().session_pending, 2);
        let blocked = first_session
            .try_retain_input(block::Height(3), 1)
            .expect_err("the session pending capacity is full");
        assert_eq!(blocked.label(), "session_pending");
        let blocked = third_session
            .try_retain_input(block::Height(3), 1)
            .expect_err("the node pending capacity is full");
        assert_eq!(blocked.label(), "node_pending");

        drop((first, second));
        assert_eq!(regulator.snapshot().node_pending, 0);
    }
}
