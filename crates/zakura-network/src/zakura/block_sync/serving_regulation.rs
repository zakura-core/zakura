//! Resource admission for serving inbound `GetBlocks` requests.
//!
//! This module turns the generic regulation primitives into one message policy.
//! A decoded request first owns bounded pending state. Its admission task then
//! reserves worst-case peer and node work before the state query starts. Unused
//! response capacity is refunded, while bytes actually queued for a peer remain
//! owned by transport frame leases until their writes finish or are dropped.

use std::sync::Arc;
#[cfg(test)]
use std::sync::Weak;

use super::serving_observation::{
    OwnershipRelease, ServingObservation, SettlementObservation, WaitObservation,
};
use super::{config::*, wire::MAX_BS_BLOCKS_PER_REQUEST, *};
use crate::zakura::regulation::{
    CommittedRateReservation, FrameLease, OutstandingByteBudget, OutstandingByteReservation,
    RateBudget, RateReservation, SlotBudget, SlotPermit,
};
use crate::zakura::transport::FrameOwnership;

/// The bounded work declaration for one decoded request.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct GetBlocksServingCost {
    /// Count after applying this node's advertised response-count cap.
    pub(super) count: u32,
    /// Worst-case encoded response payload owned until settlement or transport handoff.
    pub(super) response_cap: u64,
    /// Response capacity plus fixed byte-equivalent request work.
    pub(super) charge: u64,
}

/// Compute the worst-case work a valid request can cause using checked arithmetic.
pub(super) fn serving_cost(
    config: &ZakuraBlockSyncConfig,
    requested_count: u32,
) -> Result<GetBlocksServingCost, &'static str> {
    let count = requested_count.min(inbound_get_blocks_count_limit(config));
    let block_bytes = u64::from(count)
        .checked_mul(block::MAX_BLOCK_BYTES)
        .ok_or("GetBlocks response-cap multiplication overflowed")?;
    let bounded_block_bytes = block_bytes.min(u64::from(config.advertised_max_response_bytes()));
    let response_cap = GET_BLOCKS_TERMINAL_PAYLOAD_BYTES
        .checked_add(u64::from(count))
        .and_then(|bytes| bytes.checked_add(bounded_block_bytes))
        .ok_or("GetBlocks response-cap addition overflowed")?;
    let charge = response_cap
        .checked_add(config.get_blocks_regulation.request_overhead_bytes)
        .ok_or("GetBlocks serving charge overflowed")?;

    Ok(GetBlocksServingCost {
        count,
        response_cap,
        charge,
    })
}

/// Validate that every legal request can eventually fit every configured bound.
pub(super) fn validate_config(config: &ZakuraBlockSyncConfig) -> Result<(), &'static str> {
    if u64::from(config.max_response_bytes) < block::MAX_BLOCK_BYTES {
        return Err("max_response_bytes must cover one maximum-size block");
    }
    let regulation = &config.get_blocks_regulation;
    if regulation.request_overhead_bytes == 0 {
        return Err("get_blocks_regulation.request_overhead_bytes must be greater than zero");
    }
    if regulation.peer_rate_bytes_per_second == 0 {
        return Err("get_blocks_regulation.peer_rate_bytes_per_second must be greater than zero");
    }
    if regulation.node_rate_bytes_per_second == 0 {
        return Err("get_blocks_regulation.node_rate_bytes_per_second must be greater than zero");
    }
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

    let largest = serving_cost(config, MAX_BS_BLOCKS_PER_REQUEST)?;
    if regulation.peer_rate_capacity_bytes < largest.charge {
        return Err(
            "get_blocks_regulation.peer_rate_capacity_bytes must cover the largest legal request",
        );
    }
    if regulation.node_rate_capacity_bytes < largest.charge {
        return Err(
            "get_blocks_regulation.node_rate_capacity_bytes must cover the largest legal request",
        );
    }
    if regulation.peer_outstanding_bytes < largest.response_cap {
        return Err(
            "get_blocks_regulation.peer_outstanding_bytes must cover the largest legal response",
        );
    }
    if regulation.node_outstanding_bytes < largest.response_cap {
        return Err(
            "get_blocks_regulation.node_outstanding_bytes must cover the largest legal response",
        );
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
    config: ZakuraBlockSyncConfig,
    node_rate: RateBudget,
    node_outstanding: OutstandingByteBudget,
    node_active: SlotBudget,
    node_pending: SlotBudget,
    session_pending_capacity: usize,
    peer_rates: StdMutex<HashMap<ZakuraPeerId, Arc<PeerRateAccount>>>,
    inactive_peer_limit: usize,
    #[cfg(test)]
    sessions: StdMutex<Vec<Weak<SessionResources>>>,
}

#[derive(Debug)]
struct PeerRateAccount {
    budget: RateBudget,
}

#[derive(Debug)]
struct SessionResources {
    outstanding: OutstandingByteBudget,
    pending: SlotBudget,
    active: SlotBudget,
}

impl GetBlocksServingRegulator {
    /// Create the GetBlocks node policy from validated block-sync configuration.
    pub(super) fn new(config: ZakuraBlockSyncConfig) -> Self {
        let inactive_peer_limit = config
            .peer_limits
            .max_inbound_peers
            .saturating_add(config.peer_limits.max_outbound_peers)
            .max(1);
        debug_assert!(validate_config(&config).is_ok());
        let regulation = &config.get_blocks_regulation;
        let session_pending_capacity = pending_input_capacity_per_session(&config);
        Self {
            inner: Arc::new(RegulatorInner {
                node_rate: RateBudget::new(
                    regulation.node_rate_capacity_bytes,
                    regulation.node_rate_bytes_per_second,
                )
                .expect("GetBlocks configuration validates the node rate budget"),
                node_outstanding: OutstandingByteBudget::new(regulation.node_outstanding_bytes),
                node_active: SlotBudget::new(regulation.node_active_requests)
                    .expect("GetBlocks configuration validates the active-request capacity"),
                node_pending: SlotBudget::new(regulation.node_pending_requests)
                    .expect("GetBlocks configuration validates the pending-request capacity"),
                session_pending_capacity,
                config,
                peer_rates: StdMutex::new(HashMap::new()),
                inactive_peer_limit,
                #[cfg(test)]
                sessions: StdMutex::new(Vec::new()),
            }),
        }
    }

    /// Create one session policy while reusing a bounded identity rate account.
    pub(super) fn session(&self, peer: ZakuraPeerId, session_id: u64) -> GetBlocksServingSession {
        let peer_rate = {
            let mut accounts = self
                .inner
                .peer_rates
                .lock()
                .expect("GetBlocks peer-rate mutex should not be poisoned");
            prune_refilled_inactive_accounts(&mut accounts);
            if let Some(account) = accounts.get(&peer) {
                account.clone()
            } else {
                evict_inactive_accounts(&mut accounts, self.inner.inactive_peer_limit);
                let regulation = &self.inner.config.get_blocks_regulation;
                let account = Arc::new(PeerRateAccount {
                    budget: RateBudget::new(
                        regulation.peer_rate_capacity_bytes,
                        regulation.peer_rate_bytes_per_second,
                    )
                    .expect("GetBlocks configuration validates the peer rate budget"),
                });
                accounts.insert(peer.clone(), account.clone());
                account
            }
        };
        let resources = Arc::new(SessionResources {
            outstanding: OutstandingByteBudget::new(
                self.inner
                    .config
                    .get_blocks_regulation
                    .peer_outstanding_bytes,
            ),
            pending: SlotBudget::new(self.inner.session_pending_capacity)
                .expect("GetBlocks configuration validates the pending-request capacity"),
            active: SlotBudget::new(
                usize::try_from(self.inner.config.advertised_max_inflight_requests())
                    .expect("the GetBlocks in-flight limit fits supported targets"),
            )
            .expect("the clamped GetBlocks in-flight limit fits Tokio's semaphore"),
        });
        #[cfg(test)]
        self.inner
            .sessions
            .lock()
            .expect("GetBlocks session-resource mutex should not be poisoned")
            .push(Arc::downgrade(&resources));

        GetBlocksServingSession {
            regulator: self.clone(),
            peer,
            session_id,
            peer_rate,
            resources,
        }
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> ServingRegulationSnapshot {
        let mut sessions = self
            .inner
            .sessions
            .lock()
            .expect("GetBlocks session-resource mutex should not be poisoned");
        let mut peer_outstanding = 0u64;
        let mut max_peer_outstanding = 0u64;
        let mut session_pending = 0usize;
        sessions.retain(|session| {
            let Some(session) = session.upgrade() else {
                return false;
            };
            let outstanding = session.outstanding.reserved();
            let pending = session.pending.reserved();
            peer_outstanding = peer_outstanding.saturating_add(outstanding);
            max_peer_outstanding = max_peer_outstanding.max(outstanding);
            session_pending = session_pending.saturating_add(pending);
            true
        });
        ServingRegulationSnapshot {
            node_rate_available: self.inner.node_rate.available(),
            node_outstanding: self.inner.node_outstanding.reserved(),
            node_active: self.inner.node_active.reserved(),
            node_pending: self.inner.node_pending.reserved(),
            peer_outstanding,
            max_peer_outstanding,
            session_pending,
        }
    }
}

fn prune_refilled_inactive_accounts(accounts: &mut HashMap<ZakuraPeerId, Arc<PeerRateAccount>>) {
    accounts.retain(|_, account| {
        Arc::strong_count(account) > 1 || account.budget.available() < account.budget.capacity()
    });
}

/// Bound reconnect-persistent identities without evicting live sessions or permits.
fn evict_inactive_accounts(
    accounts: &mut HashMap<ZakuraPeerId, Arc<PeerRateAccount>>,
    inactive_limit: usize,
) {
    while accounts
        .values()
        .filter(|account| Arc::strong_count(account) == 1)
        .count()
        >= inactive_limit
    {
        let candidate = accounts
            .iter()
            .filter(|(_, account)| Arc::strong_count(account) == 1)
            .min_by_key(|(_, account)| {
                account
                    .budget
                    .capacity()
                    .saturating_sub(account.budget.available())
            })
            .map(|(peer, _)| peer.clone());
        let Some(peer) = candidate else {
            break;
        };
        accounts.remove(&peer);
    }
}

/// Per-session entry point for pending ownership and work admission.
#[derive(Clone, Debug)]
pub(super) struct GetBlocksServingSession {
    regulator: GetBlocksServingRegulator,
    peer: ZakuraPeerId,
    session_id: u64,
    peer_rate: Arc<PeerRateAccount>,
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
            release_start: OwnershipRelease::default(),
            start_height,
            count,
            observation: None,
            _session: session,
            _node: node,
            _resources: self.resources.clone(),
            release_finish: OwnershipRelease::default(),
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
            release_start: OwnershipRelease::default(),
            start_height,
            count,
            observation: None,
            _session: session,
            _node: node,
            _resources: self.resources.clone(),
            release_finish: OwnershipRelease::default(),
        }
    }

    /// Try every work bound once, rolling back earlier reservations on a block.
    pub(super) fn try_admit(
        &self,
        requested_count: u32,
    ) -> Result<AdmissionAttempt, AdmissionBlocked> {
        self.try_admit_with_slot(requested_count, None)
    }

    /// Reuse a slot delivered by a fair waiter in the next complete admission attempt.
    pub(super) fn try_admit_with_slot(
        &self,
        requested_count: u32,
        mut acquired: Option<AcquiredAdmissionSlot>,
    ) -> Result<AdmissionAttempt, AdmissionBlocked> {
        let cost = match serving_cost(&self.regulator.inner.config, requested_count) {
            Ok(cost) => cost,
            Err(error) => panic!(
                "GetBlocks serving arithmetic remains valid after configuration validation: {error}"
            ),
        };
        let peer_rate = reserve_rate(BoundKind::PeerRate, &self.peer_rate.budget, cost.charge)?;
        let node_rate = reserve_rate(
            BoundKind::NodeRate,
            &self.regulator.inner.node_rate,
            cost.charge,
        )?;
        let peer_active =
            reserve_slot(BoundKind::PeerActive, &self.resources.active, &mut acquired)?;
        let node_active = reserve_slot(
            BoundKind::NodeActive,
            &self.regulator.inner.node_active,
            &mut acquired,
        )?;
        let node_outstanding = reserve_outstanding(
            BoundKind::NodeOutstanding,
            &self.regulator.inner.node_outstanding,
            cost.response_cap,
        )?;
        let peer_outstanding = reserve_outstanding(
            BoundKind::PeerOutstanding,
            &self.resources.outstanding,
            cost.response_cap,
        )?;

        Ok(AdmissionAttempt {
            rollback_start: OwnershipRelease::default(),
            peer: self.peer.clone(),
            session_id: self.session_id,
            observation: None,
            request_overhead: self
                .regulator
                .inner
                .config
                .get_blocks_regulation
                .request_overhead_bytes,
            response_cap: cost.response_cap,
            peer_rate,
            node_rate,
            node_outstanding,
            peer_outstanding,
            _peer_active: peer_active,
            _node_active: node_active,
            _peer_rate_account: self.peer_rate.clone(),
            _session_resources: self.resources.clone(),
            rollback_finish: OwnershipRelease::default(),
        })
    }

    #[cfg(test)]
    pub(super) fn peer_rate_available(&self) -> u64 {
        self.peer_rate.budget.available()
    }
}

/// A slot assigned to this session's pending admission, retained until its retry.
#[derive(Debug)]
pub(super) struct AcquiredAdmissionSlot {
    kind: BoundKind,
    permit: SlotPermit,
}

fn reserve_slot(
    kind: BoundKind,
    budget: &SlotBudget,
    acquired: &mut Option<AcquiredAdmissionSlot>,
) -> Result<SlotPermit, AdmissionBlocked> {
    if acquired.as_ref().is_some_and(|slot| slot.kind == kind) {
        return Ok(acquired
            .take()
            .expect("the matching slot was just checked")
            .permit);
    }
    budget
        .try_reserve()
        .ok_or_else(|| AdmissionBlocked::slot(kind, budget.clone()))
}

fn reserve_rate(
    kind: BoundKind,
    budget: &RateBudget,
    bytes: u64,
) -> Result<RateReservation, AdmissionBlocked> {
    budget.try_reserve(bytes).map_err(|error| {
        debug_assert!(error.retry_after().is_some());
        AdmissionBlocked::rate(kind, budget.clone(), bytes)
    })
}

fn reserve_outstanding(
    kind: BoundKind,
    budget: &OutstandingByteBudget,
    bytes: u64,
) -> Result<OutstandingByteReservation, AdmissionBlocked> {
    match budget.try_reserve(bytes) {
        Ok(Some(reservation)) => Ok(reservation),
        Ok(None) => Err(AdmissionBlocked::outstanding(kind, budget.clone(), bytes)),
        Err(error) => panic!("validated GetBlocks response fits its outstanding budget: {error}"),
    }
}

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
    // These guards bracket the resource fields in Rust's declaration-order drop.
    release_start: OwnershipRelease,
    start_height: block::Height,
    count: u32,
    observation: Option<Arc<ServingObservation>>,
    _session: SlotPermit,
    _node: SlotPermit,
    _resources: Arc<SessionResources>,
    release_finish: OwnershipRelease,
}

impl PendingGetBlocksRequest {
    pub(super) fn observe_wait(
        &self,
        stage: &'static str,
        bound: &'static str,
    ) -> Option<WaitObservation> {
        self.observation
            .as_ref()
            .map(|observation| observation.start_wait(stage, bound))
    }

    pub(super) fn with_observation(mut self, observation: Option<Arc<ServingObservation>>) -> Self {
        (self.release_start, self.release_finish) = OwnershipRelease::pair(&observation, "pending");
        if let Some(observation) = &observation {
            observation.emit("input_retained", None);
        }
        self.observation = observation;
        self
    }

    pub(super) fn observe_admission(&self, attempt: &mut AdmissionAttempt) {
        (attempt.rollback_start, attempt.rollback_finish) =
            OwnershipRelease::pair(&self.observation, "provisional");
        if let Some(observation) = &self.observation {
            observation.emit("admission_reserved", None);
        }
        attempt.observation = self.observation.clone();
    }

    /// Count used to reserve the request's worst-case response work.
    pub(super) fn count(&self) -> u32 {
        self.count
    }

    /// End pending ownership and return the validated request fields.
    pub(super) fn into_parts(self) -> (block::Height, u32) {
        if let Some(observation) = &self.observation {
            observation.emit("input_consumed", None);
        }
        (self.start_height, self.count)
    }
}

/// The work bound that rejected an otherwise valid request.
#[derive(Clone, Debug)]
pub(super) struct AdmissionBlocked {
    kind: BoundKind,
    wait: AdmissionWait,
}

/// Stable resource names used by low-cardinality delay observations.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum BoundKind {
    PeerRate,
    NodeRate,
    PeerActive,
    NodeActive,
    NodeOutstanding,
    PeerOutstanding,
}

impl BoundKind {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::PeerRate => "peer_rate",
            Self::NodeRate => "node_rate",
            Self::PeerActive => "peer_active",
            Self::NodeActive => "node_active",
            Self::NodeOutstanding => "node_outstanding",
            Self::PeerOutstanding => "peer_outstanding",
        }
    }
}

#[derive(Clone, Debug)]
enum AdmissionWait {
    Rate {
        budget: RateBudget,
        bytes: u64,
    },
    Outstanding {
        budget: OutstandingByteBudget,
        bytes: u64,
    },
    Slot(SlotBudget),
}

impl AdmissionBlocked {
    fn rate(kind: BoundKind, budget: RateBudget, bytes: u64) -> Self {
        Self {
            kind,
            wait: AdmissionWait::Rate { budget, bytes },
        }
    }

    fn outstanding(kind: BoundKind, budget: OutstandingByteBudget, bytes: u64) -> Self {
        Self {
            kind,
            wait: AdmissionWait::Outstanding { budget, bytes },
        }
    }

    fn slot(kind: BoundKind, budget: SlotBudget) -> Self {
        Self {
            kind,
            wait: AdmissionWait::Slot(budget),
        }
    }

    pub(super) fn kind(&self) -> BoundKind {
        self.kind
    }

    /// Wait only for the bound that blocked the previous atomic attempt.
    pub(super) async fn wait(self) -> Option<AcquiredAdmissionSlot> {
        match self.wait {
            AdmissionWait::Rate { budget, bytes } => budget
                .wait_for(bytes)
                .await
                .expect("validated GetBlocks work fits the rate budget"),
            AdmissionWait::Outstanding { budget, bytes } => budget
                .wait_for(bytes)
                .await
                .expect("validated GetBlocks response fits the outstanding budget"),
            AdmissionWait::Slot(budget) => {
                return Some(AcquiredAdmissionSlot {
                    kind: self.kind,
                    permit: budget.reserve().await,
                })
            }
        }
        None
    }
}

/// Provisional ownership of every resource needed before state work starts.
#[derive(Debug)]
#[must_use = "dropping a GetBlocks admission attempt rolls back every reservation"]
pub(super) struct AdmissionAttempt {
    // Keep the rollback guards around every resource field; commit disarms them.
    rollback_start: OwnershipRelease,
    peer: ZakuraPeerId,
    session_id: u64,
    observation: Option<Arc<ServingObservation>>,
    request_overhead: u64,
    response_cap: u64,
    peer_rate: RateReservation,
    node_rate: RateReservation,
    node_outstanding: OutstandingByteReservation,
    peer_outstanding: OutstandingByteReservation,
    _peer_active: SlotPermit,
    _node_active: SlotPermit,
    _peer_rate_account: Arc<PeerRateAccount>,
    _session_resources: Arc<SessionResources>,
    rollback_finish: OwnershipRelease,
}

impl AdmissionAttempt {
    pub(super) fn peer(&self) -> &ZakuraPeerId {
        &self.peer
    }

    pub(super) fn session_id(&self) -> u64 {
        self.session_id
    }

    /// Commit fixed request work after the reactor accepts this exact session.
    pub(super) fn commit(mut self) -> GetBlocksServingPermit {
        debug_assert_eq!(
            self.peer_rate.reserved(),
            self.response_cap.saturating_add(self.request_overhead)
        );
        debug_assert_eq!(self.node_rate.reserved(), self.peer_rate.reserved());
        let peer_rate = self
            .peer_rate
            .commit(self.request_overhead)
            .expect("validated GetBlocks charge contains its request overhead");
        let node_rate = self
            .node_rate
            .commit(self.request_overhead)
            .expect("validated GetBlocks charge contains its request overhead");
        metrics::counter!("sync.block.serving.admitted").increment(1);
        self.rollback_start.disarm();
        self.rollback_finish.disarm();
        if let Some(observation) = &self.observation {
            observation.emit("committed", None);
        }
        GetBlocksServingPermit {
            query: Arc::new(QueryLifecycle::default()),
            resources: Arc::new(StdMutex::new(ServingResources {
                peer: self.peer,
                session_id: self.session_id,
                observation: self.observation,
                request_id: None,
                request_overhead: self.request_overhead,
                response_cap: self.response_cap,
                transferred: 0,
                next_frame_sequence: 0,
                peer_rate,
                node_rate,
                node_outstanding: self.node_outstanding,
                peer_outstanding: self.peer_outstanding,
                _peer_active: self._peer_active,
                _node_active: self._node_active,
                _peer_rate_account: self._peer_rate_account,
                _session_resources: self._session_resources,
                settlement: None,
            })),
        }
    }
}

/// Committed request ownership retained by the reactor's serving ledger.
#[derive(Debug)]
#[must_use = "the serving ledger must retain this permit until request settlement"]
pub(super) struct GetBlocksServingPermit {
    query: Arc<QueryLifecycle>,
    resources: Arc<StdMutex<ServingResources>>,
}

#[derive(Debug)]
struct ServingResources {
    peer: ZakuraPeerId,
    session_id: u64,
    observation: Option<Arc<ServingObservation>>,
    request_id: Option<BlockRangeRequestId>,
    request_overhead: u64,
    response_cap: u64,
    transferred: u64,
    next_frame_sequence: u64,
    peer_rate: CommittedRateReservation,
    node_rate: CommittedRateReservation,
    node_outstanding: OutstandingByteReservation,
    peer_outstanding: OutstandingByteReservation,
    _peer_active: SlotPermit,
    _node_active: SlotPermit,
    _peer_rate_account: Arc<PeerRateAccount>,
    _session_resources: Arc<SessionResources>,
    // Rust drops fields in declaration order. Keep this after every resource owner.
    settlement: Option<SettlementObservation>,
}

impl ServingResources {
    /// Bind the reactor request identity exactly once for diagnostics.
    pub(super) fn bind_request_id(&mut self, request_id: BlockRangeRequestId) {
        assert!(
            self.request_id.replace(request_id).is_none(),
            "a GetBlocks serving permit is bound to one request"
        );
        if let Some(observation) = &self.observation {
            observation.emit("request_bound", Some(request_id));
        }
    }

    /// Return whether an encoded response frame fits every remaining balance.
    pub(super) fn can_transfer_frame(&self, bytes: u64) -> bool {
        self.peer_rate.refundable() >= bytes
            && self.node_rate.refundable() >= bytes
            && self.node_outstanding.remaining() >= bytes
            && self.peer_outstanding.remaining() >= bytes
    }

    /// Transfer actual response bytes into a transport-owned frame lease.
    pub(super) fn transfer_frame(&mut self, bytes: u64) -> FrameLease {
        assert!(
            self.can_transfer_frame(bytes),
            "encoded GetBlocks response bytes stay within the admitted cap"
        );
        let lease = OutstandingByteReservation::transfer_to_frame(
            [&mut self.node_outstanding, &mut self.peer_outstanding],
            bytes,
        )
        .expect("prechecked response bytes fit both outstanding reservations");
        self.peer_rate
            .spend(bytes)
            .expect("prechecked response bytes fit the peer rate reservation");
        self.node_rate
            .spend(bytes)
            .expect("prechecked response bytes fit the node rate reservation");
        self.transferred = self
            .transferred
            .checked_add(bytes)
            .expect("transferred bytes cannot exceed the validated response cap");
        lease
    }

    #[cfg(test)]
    pub(super) fn refunded_response_bytes(&self) -> u64 {
        self.response_cap.saturating_sub(self.transferred)
    }
}

impl Drop for ServingResources {
    fn drop(&mut self) {
        self.settlement = self.observation.as_ref().map(|observation| {
            observation.start_settlement(
                self.request_id,
                self.request_overhead,
                self.response_cap,
                self.transferred,
            )
        });
        let refunded = self.response_cap.saturating_sub(self.transferred);
        metrics::counter!("sync.block.serving.refunded_bytes").increment(refunded);
        tracing::trace!(
            peer = ?self.peer,
            session_id = self.session_id,
            request_id = ?self.request_id,
            request_overhead = self.request_overhead,
            queued_bytes = self.transferred,
            refunded_bytes = refunded,
            "settled regulated GetBlocks request"
        );
    }
}

impl GetBlocksServingPermit {
    pub(super) fn bind_request_id(&mut self, request_id: BlockRangeRequestId) {
        self.resources
            .lock()
            .expect("a panic in serving accounting invalidates its balances")
            .bind_request_id(request_id);
    }

    pub(super) fn can_transfer_frame(&self, bytes: u64) -> bool {
        self.resources
            .lock()
            .expect("a panic in serving accounting invalidates its balances")
            .can_transfer_frame(bytes)
    }

    #[cfg(test)]
    pub(super) fn transfer_frame(&mut self, bytes: u64) -> FrameLease {
        self.resources
            .lock()
            .expect("a panic in serving accounting invalidates its balances")
            .transfer_frame(bytes)
    }

    /// Transfer the byte lease and attach diagnostic identity without retaining this permit.
    pub(super) fn transfer_frame_for_write(&mut self, bytes: u64) -> FrameOwnership {
        let mut resources = self
            .resources
            .lock()
            .expect("a panic in serving accounting invalidates its balances");
        let ownership = FrameOwnership::from(resources.transfer_frame(bytes));
        let Some(observation) = resources.observation.clone() else {
            return ownership;
        };
        let sequence = resources.next_frame_sequence;
        resources.next_frame_sequence = sequence
            .checked_add(1)
            .expect("nonempty response frames are bounded by the admitted response capacity");
        ownership.with_observer(observation.frame_observer(resources.request_id, sequence, bytes))
    }

    /// Share capacity with the dispatched query and its response, without charging twice.
    pub(super) fn query_lease(&self) -> BlockRangeQueryLease {
        BlockRangeQueryLease {
            _resources: self.resources.clone(),
            query: self.query.clone(),
        }
    }

    #[cfg(test)]
    pub(super) fn refunded_response_bytes(&self) -> u64 {
        self.resources
            .lock()
            .expect("test accounting is not poisoned")
            .refunded_response_bytes()
    }
}

impl Drop for GetBlocksServingPermit {
    fn drop(&mut self) {
        self.query.cancel();
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
enum QueryState {
    #[default]
    Queued,
    Started,
    Cancelled,
}

/// One ordering for execution claims and ledger closure, shared by all leases.
#[derive(Debug, Default)]
struct QueryLifecycle {
    state: StdMutex<QueryState>,
    cancelled: CancellationToken,
}

impl QueryLifecycle {
    fn try_start(&self) -> bool {
        let mut state = self.state.lock().expect("query lifecycle is not poisoned");
        if *state != QueryState::Queued {
            return false;
        }
        *state = QueryState::Started;
        true
    }

    fn cancel(&self) {
        // Close admission before waking a worker. A previously claimed read
        // still owns its resource lease and must drain to completion.
        *self.state.lock().expect("query lifecycle is not poisoned") = QueryState::Cancelled;
        self.cancelled.cancel();
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
    _resources: Arc<StdMutex<ServingResources>>,
    query: Arc<QueryLifecycle>,
}

impl BlockRangeQueryLease {
    /// Claim the only execution, serialized against ledger closure.
    ///
    /// If closure wins, no read starts. If the claim wins, the worker retains
    /// capacity until the read finishes, even if delivery is then cancelled.
    pub fn try_start(&self) -> bool {
        self.query.try_start()
    }

    /// Whether the request no longer has a live delivery owner.
    pub fn is_cancelled(&self) -> bool {
        self.query.cancelled.is_cancelled()
    }

    /// Wait for the ledger to close. This does not cancel an active state read.
    pub async fn cancelled(&self) {
        self.query.cancelled.cancelled().await;
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
    lease.query = Arc::new(QueryLifecycle::default());
    lease
}

#[cfg(test)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct ServingRegulationSnapshot {
    pub(super) node_rate_available: u64,
    pub(super) node_outstanding: u64,
    pub(super) node_active: usize,
    pub(super) node_pending: usize,
    pub(super) peer_outstanding: u64,
    pub(super) max_peer_outstanding: u64,
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
        let charged = regulator.snapshot().node_outstanding;
        drop(permit);
        assert!(query.is_cancelled());
        assert_eq!(regulator.snapshot().node_active, 1);
        assert_eq!(regulator.snapshot().node_outstanding, charged);
        drop(query);
        assert_eq!(regulator.snapshot().node_outstanding, charged);
        drop(result);
        assert_eq!(regulator.snapshot().node_active, 0);
        assert_eq!(regulator.snapshot().node_outstanding, 0);
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
            let charged = regulator.snapshot().node_outstanding;
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
            assert_eq!(regulator.snapshot().node_outstanding, charged);
            drop(lease);
            assert_eq!(regulator.snapshot().node_active, 0);
            assert_eq!(regulator.snapshot().node_outstanding, 0);
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
        let blocked = session.try_admit(1).expect_err("the active slot is owned");
        assert_eq!(blocked.kind(), BoundKind::NodeActive);
        let wait = blocked.wait();
        tokio::pin!(wait);
        assert!(poll!(&mut wait).is_pending());
        drop(owner);
        let acquired = tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("released capacity reaches its waiter");
        let admitted = session
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
                    assert_eq!(regulator.snapshot().node_outstanding, 0);
                    assert_eq!(regulator.snapshot().node_active, 0);
                    eprintln!("serving_measurement block={label} body_bytes={} peers={peers} responses={} regulated={regulated} elapsed_ms={:.3}", body_bytes, u32::from(peers) * 32, elapsed.as_secs_f64() * 1000.0);
                }
            }
        }
    }

    #[test]
    fn cost_includes_block_discriminators_terminal_and_configured_overhead() {
        let mut config = ZakuraBlockSyncConfig {
            max_blocks_per_response: 2,
            max_response_bytes: u32::try_from(block::MAX_BLOCK_BYTES * 2)
                .expect("two maximum block bodies fit u32"),
            ..ZakuraBlockSyncConfig::default()
        };
        config.get_blocks_regulation.request_overhead_bytes = 17;

        let cost = serving_cost(&config, 2).expect("the default bounds do not overflow");
        assert_eq!(cost.count, 2);
        assert_eq!(
            cost.response_cap,
            block::MAX_BLOCK_BYTES * 2 + 2 + GET_BLOCKS_TERMINAL_PAYLOAD_BYTES
        );
        assert_eq!(cost.charge, cost.response_cap + 17);

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
    fn config_rejects_a_bound_that_cannot_admit_the_largest_request() {
        let mut config = ZakuraBlockSyncConfig::default();
        let largest = serving_cost(&config, MAX_BS_BLOCKS_PER_REQUEST)
            .expect("the default cost is representable");
        config.get_blocks_regulation.node_outstanding_bytes = largest.response_cap - 1;

        assert_eq!(
            validate_config(&config),
            Err("get_blocks_regulation.node_outstanding_bytes must cover the largest legal response")
        );

        let mut small_response = ZakuraBlockSyncConfig {
            max_response_bytes: u32::try_from(block::MAX_BLOCK_BYTES).unwrap(),
            ..ZakuraBlockSyncConfig::default()
        };
        let largest = serving_cost(&small_response, MAX_BS_BLOCKS_PER_REQUEST)
            .expect("the minimum response policy is representable");
        small_response.get_blocks_regulation.peer_outstanding_bytes = largest.response_cap;
        small_response.get_blocks_regulation.node_outstanding_bytes = largest.response_cap;
        assert_eq!(
            validate_config(&small_response),
            Ok(()),
            "the minimum response cap and its framing allowance admit any single block",
        );
    }

    #[test]
    fn config_rejects_nonprogressing_or_unbounded_admission_settings() {
        let base = ZakuraBlockSyncConfig::default();

        let mut no_peer_refill = base.clone();
        no_peer_refill
            .get_blocks_regulation
            .peer_rate_bytes_per_second = 0;
        assert_eq!(
            validate_config(&no_peer_refill),
            Err("get_blocks_regulation.peer_rate_bytes_per_second must be greater than zero"),
        );

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
        let mut config = ZakuraBlockSyncConfig::default();
        let cost = serving_cost(&config, 1).expect("the default cost is representable");
        config.get_blocks_regulation.peer_outstanding_bytes = cost.response_cap;
        let regulator = GetBlocksServingRegulator::new(config.clone());
        let session = regulator.session(peer(1), 1);
        let other_peer = regulator.session(peer(7), 1);
        let first = session.try_admit(1).expect("the first request fits");
        let before = regulator.snapshot();
        let blocked = session
            .try_admit(1)
            .expect_err("the peer outstanding budget is occupied by the first request");
        assert_eq!(blocked.kind(), BoundKind::PeerOutstanding);
        assert_eq!(regulator.snapshot(), before);

        let independent = other_peer
            .try_admit(1)
            .expect("one peer's outstanding bound does not consume another peer's capacity");
        assert_eq!(regulator.snapshot().node_active, 2);
        drop(independent);
        drop(first);
    }

    #[tokio::test(start_paused = true)]
    async fn committed_request_spends_overhead_and_refunds_unused_response_work() {
        let mut config = ZakuraBlockSyncConfig::default();
        config.get_blocks_regulation.request_overhead_bytes = 11;
        let cost = serving_cost(&config, 1).expect("the default cost is representable");
        let regulator = GetBlocksServingRegulator::new(config.clone());
        let session = regulator.session(peer(2), 2);
        let full_node_rate = regulator.snapshot().node_rate_available;
        let full_peer_rate = session.peer_rate_available();

        let permit = session.try_admit(1).expect("the request fits").commit();
        assert_eq!(permit.refunded_response_bytes(), cost.response_cap);
        drop(permit);

        assert_eq!(
            regulator.snapshot().node_rate_available,
            full_node_rate - 11
        );
        assert_eq!(session.peer_rate_available(), full_peer_rate - 11);
        assert_eq!(regulator.snapshot().node_outstanding, 0);
        assert_eq!(regulator.snapshot().node_active, 0);
    }

    #[test]
    fn frame_lease_keeps_actual_bytes_outstanding_after_request_settlement() {
        let config = ZakuraBlockSyncConfig::default();
        let regulator = GetBlocksServingRegulator::new(config);
        let session = regulator.session(peer(3), 3);
        let mut permit = session.try_admit(1).expect("the request fits").commit();
        let lease = permit.transfer_frame(9);
        drop(permit);

        let snapshot = regulator.snapshot();
        assert_eq!(snapshot.node_outstanding, 9);
        assert_eq!(snapshot.peer_outstanding, 9);
        assert_eq!(snapshot.max_peer_outstanding, 9);
        assert_eq!(snapshot.node_active, 0);

        drop(lease);
        assert_eq!(regulator.snapshot().node_outstanding, 0);
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

    #[tokio::test(start_paused = true)]
    async fn peer_rate_deficit_survives_a_reconnect() {
        let mut config = ZakuraBlockSyncConfig::default();
        config.get_blocks_regulation.request_overhead_bytes = 13;
        let regulator = GetBlocksServingRegulator::new(config);
        let identity = peer(6);
        let session = regulator.session(identity.clone(), 6);
        let full = session.peer_rate_available();
        drop(session.try_admit(1).expect("the request fits").commit());
        assert_eq!(session.peer_rate_available(), full - 13);
        drop(session);

        let replacement = regulator.session(identity, 7);
        assert_eq!(replacement.peer_rate_available(), full - 13);
    }
}

#[cfg(test)]
mod properties;

#[cfg(test)]
mod workloads;
