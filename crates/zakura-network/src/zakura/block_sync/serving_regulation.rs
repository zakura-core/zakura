//! Resource admission for serving inbound `GetBlocks` requests.
//!
//! This module turns the generic regulation primitives into one message policy.
//! Each routine queues compact requests within its advertised in-flight limit.
//! One admission waiter starts work in arrival order while stream reads continue.
//! Admission acquires a response producer before the state query starts. The query,
//! result, and queued frames share that producer until the last owner drops. A blocked
//! transport writer therefore prevents another query for the same peer, including after reconnect.

use std::sync::Arc;

#[cfg(test)]
use super::wire::MAX_BS_BLOCKS_PER_REQUEST;
use super::{config::*, *};
#[cfg(test)]
use crate::zakura::regulation::WorkBound;
use crate::zakura::{
    regulation::{
        AcquiredWorkSlot, RequestAdmission, RequestSession, ResponsePermit, SlotBudget,
        WorkAttempt, WorkBlocked, WorkLease,
    },
    transport::FrameGuard,
};

mod policy;
use policy::GetBlocksPolicy;

/// Header-level size limits from the implemented message policies.
pub(super) fn message_payload_limits() -> &'static [(u16, usize)] {
    GetBlocksPolicy::PAYLOAD_LIMITS
}

/// One decoded request held at the admission boundary until work is available.
#[derive(Debug)]
pub(super) struct GetBlocksRequest {
    pub(super) start_height: block::Height,
    pub(super) count: u32,
}

impl GetBlocksRequest {
    pub(super) fn into_parts(self) -> (block::Height, u32) {
        (self.start_height, self.count)
    }
}

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
    if regulation.query_timeout < Duration::from_millis(1) {
        return Err("get_blocks_regulation.query_timeout must be at least 1ms");
    }

    Ok(())
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
}

impl GetBlocksServingRegulator {
    /// Create the GetBlocks node policy from validated block-sync configuration.
    pub(super) fn new(config: ZakuraBlockSyncConfig) -> Self {
        debug_assert!(validate_config(&config).is_ok());
        let regulation = &config.get_blocks_regulation;
        let node_active = SlotBudget::new(regulation.node_active_requests)
            .expect("GetBlocks configuration validates the active-request capacity");
        Self {
            inner: Arc::new(RegulatorInner {
                admission: RequestAdmission::new(
                    GetBlocksPolicy::new(&config),
                    node_active.clone(),
                    GetBlocksPolicy::PEER_PRODUCERS,
                ),
                #[cfg(test)]
                node_active,
            }),
        }
    }

    /// Create one session policy within the node admission bounds.
    pub(super) fn session(&self, peer: ZakuraPeerId, session_id: u64) -> GetBlocksServingSession {
        let work = self.inner.admission.session(&peer);

        GetBlocksServingSession {
            peer,
            session_id,
            work,
        }
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> ServingRegulationSnapshot {
        ServingRegulationSnapshot {
            node_active: self.inner.node_active.reserved(),
            peer_active: self.inner.admission.reserved_by_peers(),
        }
    }
}

/// Per-session entry point for decoding and work admission.
#[derive(Clone, Debug)]
pub(super) struct GetBlocksServingSession {
    peer: ZakuraPeerId,
    session_id: u64,
    work: RequestSession<GetBlocksPolicy>,
}

impl GetBlocksServingSession {
    /// Wait in peer-then-node order. The enclosing session cancels this wait.
    pub(super) async fn admit_request(&self, request: &GetBlocksRequest) -> GetBlocksServingPermit {
        AdmissionAttempt {
            peer: self.peer.clone(),
            session_id: self.session_id,
            work: self.work.admit(request).await,
        }
        .commit()
    }

    /// Apply the declared codec before admitting an inbound request.
    pub(super) fn decode_request(
        &self,
        frame: Frame,
    ) -> Result<BlockSyncMessage, BlockSyncWireError> {
        self.work
            .decode(frame)
            .map(|request| BlockSyncMessage::GetBlocks {
                start_height: request.start_height,
                count: request.count,
            })
    }

    /// Admit an already decoded, retained request before dispatching its state work.
    pub(super) fn try_admit_request(
        &self,
        request: &GetBlocksRequest,
        acquired: Option<AcquiredWorkSlot>,
    ) -> Result<AdmissionAttempt, WorkBlocked> {
        let work = self.work.try_admit(request, acquired)?;
        Ok(AdmissionAttempt {
            peer: self.peer.clone(),
            session_id: self.session_id,
            work,
        })
    }

    #[cfg(any(test, feature = "zakura-testkit"))]
    pub(super) fn try_admit(&self, count: u32) -> Result<AdmissionAttempt, WorkBlocked> {
        self.try_admit_request(
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
        acquired: Option<AcquiredWorkSlot>,
    ) -> Result<AdmissionAttempt, WorkBlocked> {
        self.try_admit_request(
            &GetBlocksRequest {
                start_height: block::Height(0),
                count,
            },
            acquired,
        )
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
    pub(super) peer_active: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(byte: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![byte; 32]).expect("test peer id is within bounds")
    }

    #[test]
    fn reconnects_share_the_peer_limit_until_old_reads_finish() {
        let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
        let original = regulator.session(peer(8), 1);
        let permit = original.try_admit(1).unwrap().commit();
        let query = permit.query_lease();
        assert!(query.try_start());
        drop(permit);
        drop(original);

        // Neither dropping the old session nor replacing it repeatedly releases
        // the work still owned by its running storage read.
        for generation in 2..=65 {
            let replacement = regulator.session(peer(8), generation);
            assert_eq!(
                replacement.try_admit(1).unwrap_err().kind(),
                WorkBound::Peer
            );
            assert_eq!(regulator.snapshot().node_active, 1);
            let other = regulator.session(peer(9), generation);
            assert!(other.try_admit(1).is_ok(), "another peer can still serve");
        }
        let replacement = regulator.session(peer(8), 66);
        drop(query);
        assert!(
            replacement.try_admit(1).is_ok(),
            "completion frees this peer's slot"
        );
        assert_eq!(regulator.snapshot().node_active, 0);
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
        assert_eq!(blocked.kind(), WorkBound::Node);
        let wait = blocked.wait();
        tokio::pin!(wait);
        assert!(poll!(&mut wait).is_pending());
        drop(owner);
        let acquired = tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("released capacity reaches its waiter");
        let admitted = waiting_session
            .try_admit_with_slot(1, Some(acquired))
            .expect("the retry retains its assigned slot")
            .commit();
        assert_eq!(regulator.snapshot().node_active, 1);
        drop(admitted);
        assert_eq!(regulator.snapshot().node_active, 0);
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
        assert_eq!(blocked.kind(), WorkBound::Peer);
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
        assert_eq!(session.try_admit(1).unwrap_err().kind(), WorkBound::Peer);
        assert!(other.try_admit(1).is_ok());
        drop(block);
        assert!(session.try_admit(1).is_err());
        drop(terminal);
        assert_eq!(regulator.snapshot().node_active, 0);
        assert!(session.try_admit(1).is_ok());
    }
}

#[cfg(test)]
mod properties;
