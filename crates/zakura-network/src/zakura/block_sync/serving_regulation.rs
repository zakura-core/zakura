//! Owned admission and response accounting for inbound `GetBlocks` requests.
//!
//! This module composes the generic regulation primitives into the block-sync
//! policy. Provisional attempts roll back completely. Once the reactor commits
//! an attempt, request overhead remains spent and response bytes move linearly
//! from reservations into transport-held frame leases.

use super::trace::{peer as trace_peer, BlockTraceEvent};
use super::{config::*, events::BlockRangeRequestId, wire::MAX_BS_BLOCKS_PER_REQUEST, *};
use crate::zakura::regulation::{
    ByteRateBucket, CommittedRateCharge, FrameLease, OutstandingByteBudget,
    OutstandingByteReservation, PendingInputBudget, PendingInputPermit, RateCharge,
    RateChargeError,
};
use crate::zakura::trace::block_sync_trace as bs_trace;
use std::sync::Weak;

/// The response and rate charge computed for one wire request.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct GetBlocksServingCost {
    /// Number of blocks after the local request-count clamp.
    pub(super) count: u32,
    /// Worst-case response payload reserved until settlement or transport handoff.
    pub(super) response_cap: u64,
    /// Response cap plus the fixed request-processing overhead.
    pub(super) charge: u64,
}

/// Compute the serving declaration using checked arithmetic only.
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
        .checked_add(GET_BLOCKS_REQUEST_OVERHEAD_BYTES)
        .ok_or("GetBlocks serving charge overflowed")?;

    Ok(GetBlocksServingCost {
        count,
        response_cap,
        charge,
    })
}

/// Validate that every legal request can eventually fit every configured bound.
pub(super) fn validate_config(config: &ZakuraBlockSyncConfig) -> Result<(), &'static str> {
    let regulation = &config.get_blocks_serving_regulation;
    if u64::from(config.advertised_max_response_bytes()) < block::MAX_BLOCK_BYTES {
        return Err("max_response_bytes must cover one maximum-size block");
    }
    if regulation.peer_rate_bytes_per_second == 0 {
        return Err(
            "get_blocks_serving_regulation.peer_rate_bytes_per_second must be greater than zero",
        );
    }
    if regulation.node_rate_bytes_per_second == 0 {
        return Err(
            "get_blocks_serving_regulation.node_rate_bytes_per_second must be greater than zero",
        );
    }
    if pending_input_capacity(config)? < pending_input_capacity_per_session(config)? {
        return Err("get_blocks_serving_regulation.node_pending_requests must cover one session pending-input window");
    }

    let largest = serving_cost(config, MAX_BS_BLOCKS_PER_REQUEST)?;
    if regulation.peer_rate_capacity_bytes < largest.charge {
        return Err("get_blocks_serving_regulation.peer_rate_capacity_bytes must cover the largest legal request charge");
    }
    if regulation.node_rate_capacity_bytes < largest.charge {
        return Err("get_blocks_serving_regulation.node_rate_capacity_bytes must cover the largest legal request charge");
    }
    if regulation.peer_backlog_bytes < largest.response_cap {
        return Err("get_blocks_serving_regulation.peer_backlog_bytes must cover the largest legal response cap");
    }
    if regulation.node_outstanding_bytes < largest.response_cap {
        return Err("get_blocks_serving_regulation.node_outstanding_bytes must cover the largest legal response cap");
    }
    Ok(())
}

/// Maximum decoded serving requests one session may retain before reactor processing.
///
/// One request can be waiting for admission while the advertised in-flight
/// limit bounds the requests queued behind it.
pub(super) fn pending_input_capacity_per_session(
    config: &ZakuraBlockSyncConfig,
) -> Result<usize, &'static str> {
    usize::try_from(config.advertised_max_inflight_requests())
        .map_err(|_| "GetBlocks in-flight limit does not fit usize")?
        .checked_add(1)
        .ok_or("GetBlocks per-session pending-input capacity overflowed")
}

/// Return the configured node-wide decoded-request bound.
pub(super) fn pending_input_capacity(
    config: &ZakuraBlockSyncConfig,
) -> Result<usize, &'static str> {
    let capacity = usize::try_from(config.get_blocks_serving_regulation.node_pending_requests)
        .map_err(|_| "GetBlocks node pending-input capacity does not fit usize")?;
    if capacity > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(
            "get_blocks_serving_regulation.node_pending_requests exceeds Tokio's semaphore limit",
        );
    }
    Ok(capacity)
}

/// Node-owned serving resources and reconnect-persistent peer rate buckets.
#[derive(Clone, Debug)]
pub(super) struct GetBlocksServingRegulator {
    inner: Arc<RegulatorInner>,
}

#[derive(Debug)]
struct RegulatorInner {
    config: ZakuraBlockSyncConfig,
    node_rate: ByteRateBucket,
    node_outstanding: OutstandingByteBudget,
    pending_inputs: PendingInputBudget,
    session_pending_capacity: usize,
    peer_rates: StdMutex<HashMap<ZakuraPeerId, Arc<PeerRateAccount>>>,
    sessions: StdMutex<Vec<Arc<SessionResources>>>,
    inactive_cache_limit: usize,
    trace: ZakuraTrace,
}

#[derive(Debug)]
struct PeerRateAccount {
    bucket: ByteRateBucket,
}

/// One active session or permit reference to an identity bucket.
#[derive(Debug)]
struct PeerRateReference {
    regulator: Weak<RegulatorInner>,
    peer: ZakuraPeerId,
    account: Arc<PeerRateAccount>,
    #[cfg(test)]
    drop_hook: Option<Arc<PeerRateDropHook>>,
}

#[cfg(test)]
#[derive(Debug)]
struct PeerRateDropHook {
    reached: std::sync::Barrier,
    resume: std::sync::Barrier,
}

#[cfg(test)]
impl PeerRateDropHook {
    fn new() -> Self {
        Self {
            reached: std::sync::Barrier::new(2),
            resume: std::sync::Barrier::new(2),
        }
    }

    fn pause_before_cache_lock(&self) {
        self.reached.wait();
        self.resume.wait();
    }
}

impl Clone for PeerRateReference {
    fn clone(&self) -> Self {
        Self {
            regulator: self.regulator.clone(),
            peer: self.peer.clone(),
            account: self.account.clone(),
            #[cfg(test)]
            drop_hook: self.drop_hook.clone(),
        }
    }
}

impl Drop for PeerRateReference {
    fn drop(&mut self) {
        let Some(regulator) = self.regulator.upgrade() else {
            return;
        };
        #[cfg(test)]
        if let Some(hook) = &self.drop_hook {
            hook.pause_before_cache_lock();
        }
        let mut cache = regulator
            .peer_rates
            .lock()
            .expect("GetBlocks peer-rate cache mutex should not be poisoned");
        let owns_cached_entry = cache
            .get(&self.peer)
            .is_some_and(|cached| Arc::ptr_eq(cached, &self.account));
        // The cache and this value must still be the final references while the
        // cache is locked. A replacement session cannot clone the account until
        // this check and any resulting eviction are complete.
        if !owns_cached_entry || Arc::strong_count(&self.account) != 2 {
            return;
        }
        if self.account.bucket.balance() == self.account.bucket.capacity() {
            cache.remove(&self.peer);
            return;
        }
        evict_newly_inactive_to_limit(
            &mut cache,
            regulator.inactive_cache_limit,
            &self.peer,
            &self.account,
        );
    }
}

#[derive(Debug)]
struct SessionResources {
    backlog: OutstandingByteBudget,
    pending_inputs: PendingInputBudget,
}

impl GetBlocksServingRegulator {
    /// Construct the node resources with the handler's exact connection limit.
    #[cfg(test)]
    pub(super) fn new(config: ZakuraBlockSyncConfig, max_connections: usize) -> Self {
        Self::with_trace(config, max_connections, ZakuraTrace::noop())
    }

    /// Construct production resources with block-sync JSONL observability.
    pub(super) fn with_trace(
        config: ZakuraBlockSyncConfig,
        max_connections: usize,
        trace: ZakuraTrace,
    ) -> Self {
        debug_assert!(validate_config(&config).is_ok());
        let regulation = &config.get_blocks_serving_regulation;
        let session_pending_capacity = pending_input_capacity_per_session(&config)
            .expect("the clamped GetBlocks in-flight limit fits supported targets");
        let node_pending_capacity = pending_input_capacity(&config)
            .expect("the validated GetBlocks node pending-input limit fits supported targets");
        Self {
            inner: Arc::new(RegulatorInner {
                node_rate: ByteRateBucket::new(
                    regulation.node_rate_capacity_bytes,
                    regulation.node_rate_bytes_per_second,
                ),
                node_outstanding: OutstandingByteBudget::new(regulation.node_outstanding_bytes),
                pending_inputs: PendingInputBudget::new(node_pending_capacity)
                    .expect("the validated GetBlocks pending-input limit fits Tokio's semaphore"),
                session_pending_capacity,
                config,
                peer_rates: StdMutex::new(HashMap::new()),
                sessions: StdMutex::new(Vec::new()),
                inactive_cache_limit: max_connections.max(1),
                trace,
            }),
        }
    }

    /// Create per-session backlog accounting while reusing this identity's rate bucket.
    pub(super) fn session(&self, peer: ZakuraPeerId, session_id: u64) -> GetBlocksServingSession {
        let peer_account = {
            let mut cache = self
                .inner
                .peer_rates
                .lock()
                .expect("GetBlocks peer-rate cache mutex should not be poisoned");
            prune_full_inactive(&mut cache);
            if let Some(account) = cache.get(&peer) {
                account.clone()
            } else {
                evict_inactive_to_limit(&mut cache, self.inner.inactive_cache_limit);
                let regulation = &self.inner.config.get_blocks_serving_regulation;
                let account = Arc::new(PeerRateAccount {
                    bucket: ByteRateBucket::new(
                        regulation.peer_rate_capacity_bytes,
                        regulation.peer_rate_bytes_per_second,
                    ),
                });
                cache.insert(peer.clone(), account.clone());
                account
            }
        };
        let backlog_capacity = self
            .inner
            .config
            .get_blocks_serving_regulation
            .peer_backlog_bytes;
        let session_resources = Arc::new(SessionResources {
            backlog: OutstandingByteBudget::new(backlog_capacity),
            pending_inputs: PendingInputBudget::new(self.inner.session_pending_capacity)
                .expect("the clamped GetBlocks in-flight limit fits Tokio's semaphore"),
        });
        self.inner
            .sessions
            .lock()
            .expect("GetBlocks session backlog mutex should not be poisoned")
            .push(session_resources.clone());
        let peer_account = PeerRateReference {
            regulator: Arc::downgrade(&self.inner),
            peer: peer.clone(),
            account: peer_account,
            #[cfg(test)]
            drop_hook: None,
        };
        GetBlocksServingSession {
            regulator: self.clone(),
            peer,
            session_id,
            peer_account,
            session_resources,
        }
    }

    /// Return node and peer accounting for metrics and contract assertions.
    pub(super) fn snapshot(&self) -> ServingRegulationSnapshot {
        let cached_peer_identities = self
            .inner
            .peer_rates
            .lock()
            .expect("GetBlocks peer-rate cache mutex should not be poisoned")
            .len();
        let node_capacity = self.inner.node_rate.capacity();
        let node_balance = self.inner.node_rate.balance();
        let mut sessions = self
            .inner
            .sessions
            .lock()
            .expect("GetBlocks session backlog mutex should not be poisoned");
        let mut aggregate_peer_backlog = 0u64;
        let mut max_peer_backlog = 0u64;
        let mut aggregate_session_pending_inputs = 0usize;
        let mut max_session_pending_inputs = 0usize;
        sessions.retain(|resources| {
            let backlog = resources.backlog.reserved();
            let pending_inputs = resources.pending_inputs.reserved();
            aggregate_peer_backlog = aggregate_peer_backlog.saturating_add(backlog);
            max_peer_backlog = max_peer_backlog.max(backlog);
            aggregate_session_pending_inputs =
                aggregate_session_pending_inputs.saturating_add(pending_inputs);
            max_session_pending_inputs = max_session_pending_inputs.max(pending_inputs);
            // The list is the final strong owner after a session and its
            // permits end. Keep that retired session while transport-held
            // frames or pre-reactor requests still own its resources.
            Arc::strong_count(resources) > 1 || backlog > 0 || pending_inputs > 0
        });
        drop(sessions);
        // Admissions and frame drops update the node and peer budgets as two
        // individually atomic operations. A concurrent snapshot may land
        // between them, so their independently bounded samples can differ for
        // that instant. Contract tests compare them only after a settle barrier.
        ServingRegulationSnapshot {
            node_rate_balance: node_balance,
            node_rate_capacity: node_capacity,
            node_outstanding: self.inner.node_outstanding.reserved(),
            node_outstanding_capacity: self.inner.node_outstanding.capacity(),
            pending_inputs: self.inner.pending_inputs.reserved(),
            pending_input_capacity: self.inner.pending_inputs.capacity(),
            aggregate_session_pending_inputs,
            max_session_pending_inputs,
            session_pending_input_capacity: self.inner.session_pending_capacity,
            aggregate_peer_backlog,
            max_peer_backlog,
            cached_peer_identities,
        }
    }

    #[cfg(test)]
    /// Return one cached identity's refilled balance for contract assertions.
    pub(super) fn peer_rate_balance(&self, peer: &ZakuraPeerId) -> Option<u64> {
        self.inner
            .peer_rates
            .lock()
            .expect("GetBlocks peer-rate cache mutex should not be poisoned")
            .get(peer)
            .map(|account| account.bucket.balance())
    }
}

/// Drop inactive identities whose buckets have completely refilled.
fn prune_full_inactive(cache: &mut HashMap<ZakuraPeerId, Arc<PeerRateAccount>>) {
    cache.retain(|_, account| {
        Arc::strong_count(account) > 1 || account.bucket.balance() < account.bucket.capacity()
    });
}

/// Make room before inserting a new identity without evicting active accounts.
///
/// The account with the smallest deficit is cheapest for an attacker to reset,
/// so evicting it minimizes the burst allowance regained through cache churn.
fn evict_inactive_to_limit(
    cache: &mut HashMap<ZakuraPeerId, Arc<PeerRateAccount>>,
    inactive_cache_limit: usize,
) {
    while cache
        .values()
        .filter(|account| Arc::strong_count(account) == 1)
        .count()
        >= inactive_cache_limit
    {
        let candidate = cache
            .iter()
            .filter(|(_, account)| Arc::strong_count(account) == 1)
            .min_by_key(|(_, account)| {
                account
                    .bucket
                    .capacity()
                    .saturating_sub(account.bucket.balance())
            })
            .map(|(peer, _)| peer.clone());
        let Some(peer) = candidate else {
            break;
        };
        cache.remove(&peer);
    }
}

/// Enforce the inactive bound while the last session reference is being dropped.
///
/// The account being dropped still has two strong references: this temporary
/// owner and the cache. `newly_inactive_account` therefore counts as inactive
/// even though ordinary inactive entries have only their cache reference.
fn evict_newly_inactive_to_limit(
    cache: &mut HashMap<ZakuraPeerId, Arc<PeerRateAccount>>,
    inactive_cache_limit: usize,
    newly_inactive_peer: &ZakuraPeerId,
    newly_inactive_account: &Arc<PeerRateAccount>,
) {
    let is_inactive = |peer: &ZakuraPeerId, account: &Arc<PeerRateAccount>| {
        Arc::strong_count(account) == 1
            || (peer == newly_inactive_peer
                && Arc::ptr_eq(account, newly_inactive_account)
                && Arc::strong_count(account) == 2)
    };
    while cache
        .iter()
        .filter(|(peer, account)| is_inactive(peer, account))
        .count()
        > inactive_cache_limit
    {
        let candidate = cache
            .iter()
            .filter(|(peer, account)| is_inactive(peer, account))
            .min_by_key(|(_, account)| {
                account
                    .bucket
                    .capacity()
                    .saturating_sub(account.bucket.balance())
            })
            .map(|(peer, _)| peer.clone());
        let Some(peer) = candidate else {
            break;
        };
        cache.remove(&peer);
    }
}

/// Per-session admission handle. Clones share its backlog and peer identity bucket.
#[derive(Clone, Debug)]
pub(super) struct GetBlocksServingSession {
    regulator: GetBlocksServingRegulator,
    peer: ZakuraPeerId,
    session_id: u64,
    peer_account: PeerRateReference,
    session_resources: Arc<SessionResources>,
}

impl GetBlocksServingSession {
    /// Reserve ownership for one decoded request until the reactor processes it.
    ///
    /// The session permit enforces the peer-local queue bound. The node permit
    /// bounds the same retained state across live and draining sessions.
    pub(super) fn try_retain_input(
        &self,
        start_height: block::Height,
        count: u32,
    ) -> Result<PendingGetBlocksRequest, PendingInputBlocked> {
        let session = self
            .session_resources
            .pending_inputs
            .try_reserve()
            .ok_or(PendingInputBlocked::Session)?;
        let node = self
            .regulator
            .inner
            .pending_inputs
            .try_reserve()
            .ok_or(PendingInputBlocked::Node)?;
        Ok(PendingGetBlocksRequest {
            start_height,
            count,
            _pending_input: PendingServingInputPermit {
                _session: session,
                _node: node,
                _session_resources: self.session_resources.clone(),
            },
        })
    }

    /// Try every admission bound in declaration order, rolling back on the first block.
    pub(super) fn try_admit(
        &self,
        requested_count: u32,
    ) -> Result<AdmissionAttempt, AdmissionBlocked> {
        let cost = match serving_cost(&self.regulator.inner.config, requested_count) {
            Ok(cost) => cost,
            Err(error) => panic!(
                "validated GetBlocks configuration keeps serving arithmetic in range: {error}"
            ),
        };
        let peer_rate = self
            .peer_account
            .account
            .bucket
            .try_charge(cost.charge)
            .map_err(|error| {
                AdmissionBlocked::rate(
                    BoundKind::PeerRate,
                    self.peer_account.account.bucket.clone(),
                    cost.charge,
                    error,
                )
            })?;
        let node_rate = self
            .regulator
            .inner
            .node_rate
            .try_charge(cost.charge)
            .map_err(|error| {
                AdmissionBlocked::rate(
                    BoundKind::NodeRate,
                    self.regulator.inner.node_rate.clone(),
                    cost.charge,
                    error,
                )
            })?;
        let node_outstanding = match self
            .regulator
            .inner
            .node_outstanding
            .try_reserve_owned(cost.response_cap)
        {
            Ok(Some(reservation)) => reservation,
            Ok(None) => {
                return Err(AdmissionBlocked::outstanding(
                    BoundKind::NodeOutstanding,
                    self.regulator.inner.node_outstanding.clone(),
                    cost.response_cap,
                ));
            }
            Err(error) => {
                panic!("validated node outstanding capacity covers every legal response: {error}")
            }
        };
        let peer_backlog = match self
            .session_resources
            .backlog
            .try_reserve_owned(cost.response_cap)
        {
            Ok(Some(reservation)) => reservation,
            Ok(None) => {
                return Err(AdmissionBlocked::outstanding(
                    BoundKind::PeerBacklog,
                    self.session_resources.backlog.clone(),
                    cost.response_cap,
                ));
            }
            Err(error) => {
                panic!("validated peer backlog covers every legal response: {error}")
            }
        };

        Ok(AdmissionAttempt {
            peer: self.peer.clone(),
            session_id: self.session_id,
            peer_account: self.peer_account.clone(),
            session_resources: self.session_resources.clone(),
            peer_rate,
            node_rate,
            node_outstanding,
            peer_backlog,
            response_cap: cost.response_cap,
            charge: cost.charge,
            trace: self.regulator.inner.trace.clone(),
        })
    }

    /// Record the resource that currently delays this peer's decoded request.
    pub(super) fn trace_delayed(&self, requested_count: u32, blocked: &AdmissionBlocked) {
        let charge = serving_cost(&self.regulator.inner.config, requested_count)
            .expect("validated GetBlocks configuration keeps serving arithmetic in range")
            .charge;
        metrics::counter!("sync.block.serving.delayed", "bound" => blocked.kind().label())
            .increment(1);
        let peer = trace_peer(&self.peer);
        let reason = blocked.kind().label();
        self.regulator.inner.trace.emit_event(|| {
            BlockTraceEvent::build(bs_trace::BLOCK_SERVING_DELAYED, |row| {
                row.peer = Some(peer);
                row.range_count = Some(u64::from(requested_count));
                row.estimated_bytes = Some(charge);
                row.reason = Some(reason);
            })
        });
    }

    #[cfg(test)]
    pub(super) fn backlog_reserved(&self) -> u64 {
        self.session_resources.backlog.reserved()
    }

    #[cfg(test)]
    pub(super) fn peer_rate_balance(&self) -> u64 {
        self.peer_account.account.bucket.balance()
    }

    #[cfg(test)]
    pub(super) fn peer_rate_capacity(&self) -> u64 {
        self.peer_account.account.bucket.capacity()
    }
}

/// Scope that refused ownership of another decoded serving request.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum PendingInputBlocked {
    /// This session already retains its active admission and advertised queue.
    Session,
    /// Live and draining sessions collectively reached the derived node bound.
    Node,
}

impl PendingInputBlocked {
    /// Stable low-cardinality label used by the drop metric.
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Node => "node",
        }
    }
}

/// A decoded request and the capacity that permits retaining it before the reactor.
#[derive(Debug)]
#[must_use = "a retained GetBlocks request must be processed or explicitly dropped"]
pub(super) struct PendingGetBlocksRequest {
    start_height: block::Height,
    count: u32,
    _pending_input: PendingServingInputPermit,
}

impl PendingGetBlocksRequest {
    /// Requested count needed while the admission task acquires byte ownership.
    pub(super) fn count(&self) -> u32 {
        self.count
    }

    /// End pre-reactor ownership and return the decoded request fields.
    pub(super) fn into_parts(self) -> (block::Height, u32) {
        (self.start_height, self.count)
    }
}

#[derive(Debug)]
struct PendingServingInputPermit {
    _session: PendingInputPermit,
    _node: PendingInputPermit,
    // Keep the session resource record observable until both permits release.
    _session_resources: Arc<SessionResources>,
}

/// The resource that prevented admission. It carries the exact race-free wait handle.
#[derive(Clone, Debug)]
pub(super) struct AdmissionBlocked {
    kind: BoundKind,
    wait: AdmissionWait,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum BoundKind {
    PeerRate,
    NodeRate,
    NodeOutstanding,
    PeerBacklog,
}

impl BoundKind {
    /// Stable low-cardinality label used by metrics and trace rows.
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::PeerRate => "peer_rate",
            Self::NodeRate => "node_rate",
            Self::NodeOutstanding => "node_outstanding",
            Self::PeerBacklog => "peer_backlog",
        }
    }
}

#[derive(Clone, Debug)]
enum AdmissionWait {
    Rate {
        bucket: ByteRateBucket,
        bytes: u64,
    },
    Outstanding {
        budget: OutstandingByteBudget,
        bytes: u64,
    },
}

impl AdmissionBlocked {
    fn rate(kind: BoundKind, bucket: ByteRateBucket, bytes: u64, error: RateChargeError) -> Self {
        debug_assert!(error.retry_after().is_some());
        Self {
            kind,
            wait: AdmissionWait::Rate { bucket, bytes },
        }
    }

    fn outstanding(kind: BoundKind, budget: OutstandingByteBudget, bytes: u64) -> Self {
        Self {
            kind,
            wait: AdmissionWait::Outstanding { budget, bytes },
        }
    }

    pub(super) fn kind(&self) -> BoundKind {
        self.kind
    }

    /// Wait only on the bound that rejected the preceding attempt.
    pub(super) async fn wait(self) {
        match self.wait {
            AdmissionWait::Rate { bucket, bytes } => bucket
                .wait_for(bytes)
                .await
                .expect("validated request charge fits the configured rate bucket"),
            AdmissionWait::Outstanding { budget, bytes } => budget
                .wait_for(bytes)
                .await
                .expect("validated response cap fits the configured outstanding budget"),
        }
    }
}

/// Provisional ownership of every resource needed by one request.
#[derive(Debug)]
#[must_use = "dropping a GetBlocks admission attempt rolls back every charge"]
pub(super) struct AdmissionAttempt {
    peer: ZakuraPeerId,
    session_id: u64,
    peer_account: PeerRateReference,
    session_resources: Arc<SessionResources>,
    peer_rate: RateCharge,
    node_rate: RateCharge,
    node_outstanding: OutstandingByteReservation,
    peer_backlog: OutstandingByteReservation,
    response_cap: u64,
    charge: u64,
    trace: ZakuraTrace,
}

impl AdmissionAttempt {
    /// Authenticated identity that created this provisional admission.
    pub(super) fn peer(&self) -> &ZakuraPeerId {
        &self.peer
    }

    /// Session generation that must still own the peer before commit.
    pub(super) fn session_id(&self) -> u64 {
        self.session_id
    }

    #[cfg(test)]
    /// Complete provisional rate charge used by contract assertions.
    pub(super) fn charge(&self) -> u64 {
        self.charge
    }

    /// Commit request overhead after the reactor accepts this session generation.
    pub(super) fn commit(self) -> GetBlocksServingPermit {
        debug_assert_eq!(
            self.charge.saturating_sub(self.response_cap),
            GET_BLOCKS_REQUEST_OVERHEAD_BYTES
        );
        let peer_rate = self
            .peer_rate
            .commit(GET_BLOCKS_REQUEST_OVERHEAD_BYTES)
            .expect("request overhead is part of the validated peer charge");
        let node_rate = self
            .node_rate
            .commit(GET_BLOCKS_REQUEST_OVERHEAD_BYTES)
            .expect("request overhead is part of the validated node charge");
        GetBlocksServingPermit {
            peer: self.peer,
            session_id: self.session_id,
            request_id: None,
            _peer_account: self.peer_account,
            _session_resources: self.session_resources,
            peer_rate,
            node_rate,
            node_outstanding: self.node_outstanding,
            peer_backlog: self.peer_backlog,
            response_cap: self.response_cap,
            transferred: 0,
            trace: self.trace,
        }
    }
}

/// Committed request ownership stored in the serving ledger.
#[derive(Debug)]
#[must_use = "the serving ledger must retain the permit until request settlement"]
pub(super) struct GetBlocksServingPermit {
    peer: ZakuraPeerId,
    session_id: u64,
    request_id: Option<BlockRangeRequestId>,
    _peer_account: PeerRateReference,
    _session_resources: Arc<SessionResources>,
    peer_rate: CommittedRateCharge,
    node_rate: CommittedRateCharge,
    node_outstanding: OutstandingByteReservation,
    peer_backlog: OutstandingByteReservation,
    response_cap: u64,
    transferred: u64,
    trace: ZakuraTrace,
}

impl GetBlocksServingPermit {
    /// Bind the reactor-allocated request id exactly once.
    pub(super) fn bind_request_id(&mut self, request_id: BlockRangeRequestId) {
        assert!(
            self.request_id.replace(request_id).is_none(),
            "a GetBlocks serving permit is bound to one request id"
        );
    }

    /// Return whether this frame still fits every linear response balance.
    pub(super) fn can_transfer_frame(&self, bytes: u64) -> bool {
        self.peer_rate.refundable() >= bytes
            && self.node_rate.refundable() >= bytes
            && self.node_outstanding.remaining() >= bytes
            && self.peer_backlog.remaining() >= bytes
    }

    /// Move one encoded frame's payload bytes into a transport-held lease.
    pub(super) fn transfer_frame(&mut self, bytes: u64) -> FrameLease {
        assert!(
            self.can_transfer_frame(bytes),
            "encoded response bytes stay within every declared response balance"
        );
        let lease = OutstandingByteReservation::transfer_all(
            [&mut self.node_outstanding, &mut self.peer_backlog],
            bytes,
        )
        .expect("encoded response bytes stay within both response reservations");
        self.peer_rate
            .record_usage(bytes)
            .expect("prechecked peer rate usage fits the committed charge");
        self.node_rate
            .record_usage(bytes)
            .expect("prechecked node rate usage fits the committed charge");
        self.transferred = self
            .transferred
            .checked_add(bytes)
            .expect("transferred response bytes cannot exceed the validated response cap");
        lease
    }

    /// Response capacity that will return to both rate buckets on settlement.
    pub(super) fn refunded(&self) -> u64 {
        self.response_cap.saturating_sub(self.transferred)
    }
}

impl Drop for GetBlocksServingPermit {
    fn drop(&mut self) {
        let refunded = self.refunded();
        metrics::counter!("sync.block.serving.refunded_bytes").increment(refunded);
        tracing::trace!(
            peer = ?self.peer,
            session_id = self.session_id,
            request_id = ?self.request_id,
            queued_bytes = self.transferred,
            refunded_bytes = refunded,
            "settled regulated GetBlocks serving request"
        );
        let peer = trace_peer(&self.peer);
        let request_id = self.request_id.map(|id| id.get());
        let transferred = self.transferred;
        let charge = self
            .response_cap
            .saturating_add(GET_BLOCKS_REQUEST_OVERHEAD_BYTES);
        self.trace.emit_event(|| {
            BlockTraceEvent::build(bs_trace::BLOCK_SERVING_SETTLED, |row| {
                row.peer = Some(peer);
                row.request_id = request_id;
                row.estimated_bytes = Some(charge);
                row.serialized_bytes = Some(transferred);
                row.released_bytes = Some(refunded);
            })
        });
    }
}

/// Node-level values used by bounded-load assertions and gauges.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct ServingRegulationSnapshot {
    /// Tokens currently available in the node-wide rate bucket.
    pub(super) node_rate_balance: u64,
    /// Maximum node-wide rate burst.
    pub(super) node_rate_capacity: u64,
    /// Response bytes still owned by requests or queued frame leases.
    pub(super) node_outstanding: u64,
    /// Maximum application-owned response bytes across the node.
    pub(super) node_outstanding_capacity: u64,
    /// Decoded requests retained before reactor processing across the node.
    pub(super) pending_inputs: usize,
    /// Derived node-wide cap on retained decoded requests.
    pub(super) pending_input_capacity: usize,
    /// Sum of retained decoded requests attributed to session budgets.
    pub(super) aggregate_session_pending_inputs: usize,
    /// Largest number of pre-reactor requests retained by one session.
    pub(super) max_session_pending_inputs: usize,
    /// Per-session cap: one active admission plus the advertised queue.
    pub(super) session_pending_input_capacity: usize,
    /// Sum of response bytes held by all live and draining sessions.
    pub(super) aggregate_peer_backlog: u64,
    /// Largest response backlog held by one session.
    pub(super) max_peer_backlog: u64,
    /// Active and retained authenticated identity buckets.
    pub(super) cached_peer_identities: usize,
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    const ORACLE_REQUEST_COUNT_CAP: u32 = 128;
    const ORACLE_RESPONSE_BYTE_CAP: u32 = 32 * 1024 * 1024;
    const ORACLE_REQUEST_OVERHEAD_BYTES: u64 = 64 * 1024;
    const ORACLE_TERMINAL_PAYLOAD_BYTES: u64 = 9;

    fn oracle_serving_cost(
        requested_count: u32,
        configured_count_limit: u32,
        configured_response_limit: u32,
    ) -> (u32, u64, u64) {
        let local_count_limit = configured_count_limit.clamp(1, ORACLE_REQUEST_COUNT_CAP);
        let count = requested_count.min(local_count_limit);
        let response_byte_limit = configured_response_limit.clamp(1, ORACLE_RESPONSE_BYTE_CAP);
        let body_bytes = u64::from(count)
            .checked_mul(block::MAX_BLOCK_BYTES)
            .expect("the independent wire-sized product fits u64")
            .min(u64::from(response_byte_limit));
        let response_cap = ORACLE_TERMINAL_PAYLOAD_BYTES
            .checked_add(u64::from(count))
            .and_then(|bytes| bytes.checked_add(body_bytes))
            .expect("the independent wire-sized response cap fits u64");
        let charge = response_cap
            .checked_add(ORACLE_REQUEST_OVERHEAD_BYTES)
            .expect("the independent wire-sized charge fits u64");
        (count, response_cap, charge)
    }

    fn peer(byte: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![byte; 32]).expect("32-byte test peer id is valid")
    }

    proptest! {
        #[test]
        fn gb_rl_01_charge_matches_declared_formula(
            requested_count in any::<u32>(),
            local_count_limit in any::<u32>(),
            response_byte_limit in any::<u32>(),
        ) {
            prop_assert_eq!(MAX_BS_BLOCKS_PER_REQUEST, ORACLE_REQUEST_COUNT_CAP);
            prop_assert_eq!(MAX_BS_RESPONSE_BYTES, ORACLE_RESPONSE_BYTE_CAP);
            prop_assert_eq!(GET_BLOCKS_REQUEST_OVERHEAD_BYTES, ORACLE_REQUEST_OVERHEAD_BYTES);
            prop_assert_eq!(GET_BLOCKS_TERMINAL_PAYLOAD_BYTES, ORACLE_TERMINAL_PAYLOAD_BYTES);

            let requested_counts = [0, 1, 127, 128, 129, u32::MAX, requested_count];
            let local_limits = [0, 1, 127, 128, 129, u32::MAX, local_count_limit];
            let response_limits = [
                0,
                1,
                ORACLE_RESPONSE_BYTE_CAP - 1,
                ORACLE_RESPONSE_BYTE_CAP,
                ORACLE_RESPONSE_BYTE_CAP + 1,
                u32::MAX,
                response_byte_limit,
            ];

            for requested_count in requested_counts {
                for local_count_limit in local_limits {
                    for response_byte_limit in response_limits {
                        let config = ZakuraBlockSyncConfig {
                            max_blocks_per_response: local_count_limit,
                            max_response_bytes: response_byte_limit,
                            ..ZakuraBlockSyncConfig::default()
                        };
                        let actual = serving_cost(&config, requested_count)
                            .expect("wire-sized GetBlocks arithmetic fits u64");
                        let expected = oracle_serving_cost(
                            requested_count,
                            local_count_limit,
                            response_byte_limit,
                        );
                        prop_assert_eq!((actual.count, actual.response_cap, actual.charge), expected);
                    }
                }
            }
        }
    }

    #[test]
    fn gb_rl_03_attempt_rolls_back_and_commit_keeps_overhead() {
        let mut config = ZakuraBlockSyncConfig::default();
        config
            .get_blocks_serving_regulation
            .peer_rate_bytes_per_second = 1;
        config
            .get_blocks_serving_regulation
            .node_rate_bytes_per_second = 1;
        let regulator = GetBlocksServingRegulator::new(config.clone(), 2);
        let session = regulator.session(peer(1), 7);
        let node_before = regulator.snapshot().node_rate_balance;
        let attempt = session.try_admit(1).expect("first request fits");
        let charge = attempt.charge();
        drop(attempt);
        assert_eq!(regulator.snapshot().node_rate_balance, node_before);

        let attempt = session
            .try_admit(1)
            .expect("rolled-back request fits again");
        let mut permit = attempt.commit();
        let terminal = permit.transfer_frame(GET_BLOCKS_TERMINAL_PAYLOAD_BYTES);
        drop(permit);
        assert_eq!(
            regulator.snapshot().node_rate_balance,
            node_before - GET_BLOCKS_REQUEST_OVERHEAD_BYTES - GET_BLOCKS_TERMINAL_PAYLOAD_BYTES
        );
        assert_eq!(
            regulator.snapshot().node_outstanding,
            GET_BLOCKS_TERMINAL_PAYLOAD_BYTES,
            "queued bytes remain outstanding with their frame lease"
        );
        drop(terminal);
        assert!(charge > GET_BLOCKS_REQUEST_OVERHEAD_BYTES);
        assert_eq!(session.backlog_reserved(), 0);
        assert_eq!(regulator.snapshot().node_outstanding, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn gb_rl_07_stalled_outstanding_bytes_do_not_refill_with_time() {
        let config = ZakuraBlockSyncConfig::default();
        let regulator = GetBlocksServingRegulator::new(config, 2);
        let session = regulator.session(peer(2), 8);
        let attempt = session.try_admit(1).expect("first request fits");
        let reserved = regulator.snapshot().node_outstanding;
        assert!(reserved > 0);
        tokio::time::advance(Duration::from_secs(24 * 60 * 60)).await;
        assert_eq!(regulator.snapshot().node_outstanding, reserved);
        assert!(regulator.snapshot().node_rate_balance <= regulator.snapshot().node_rate_capacity);
        drop(attempt);
        assert_eq!(regulator.snapshot().node_outstanding, 0);
    }

    proptest! {
        #[test]
        fn gb_rl_12_supported_configuration_covers_largest_request(
            local_count_limit in any::<u32>(),
            local_inflight_limit in any::<u32>(),
            response_byte_limit in u32::try_from(block::MAX_BLOCK_BYTES)
                .expect("maximum block bytes fit u32")..=MAX_BS_RESPONSE_BYTES,
        ) {
            let mut config = ZakuraBlockSyncConfig {
                max_blocks_per_response: local_count_limit,
                max_inflight_requests: local_inflight_limit,
                max_response_bytes: response_byte_limit,
                ..ZakuraBlockSyncConfig::default()
            };
            let (_, response_cap, charge) = oracle_serving_cost(
                ORACLE_REQUEST_COUNT_CAP,
                local_count_limit,
                response_byte_limit,
            );
            config.get_blocks_serving_regulation.peer_rate_capacity_bytes = charge;
            config.get_blocks_serving_regulation.node_rate_capacity_bytes = charge;
            config.get_blocks_serving_regulation.peer_backlog_bytes = response_cap;
            config.get_blocks_serving_regulation.node_outstanding_bytes = response_cap;
            config.get_blocks_serving_regulation.node_pending_requests = config
                .advertised_max_inflight_requests()
                .saturating_add(1);
            prop_assert!(validate_config(&config).is_ok());

            let mut invalid = config.clone();
            invalid.get_blocks_serving_regulation.node_pending_requests =
                config.advertised_max_inflight_requests();
            prop_assert!(validate_config(&invalid).is_err());

            let mut invalid = config.clone();
            invalid.get_blocks_serving_regulation.peer_rate_bytes_per_second = 0;
            prop_assert!(validate_config(&invalid).is_err());

            let mut invalid = config.clone();
            invalid.get_blocks_serving_regulation.node_rate_bytes_per_second = 0;
            prop_assert!(validate_config(&invalid).is_err());

            let mut invalid = config.clone();
            invalid.get_blocks_serving_regulation.peer_rate_capacity_bytes =
                charge.saturating_sub(1);
            prop_assert!(validate_config(&invalid).is_err());

            let mut invalid = config.clone();
            invalid.get_blocks_serving_regulation.node_rate_capacity_bytes =
                charge.saturating_sub(1);
            prop_assert!(validate_config(&invalid).is_err());

            let mut invalid = config.clone();
            invalid.get_blocks_serving_regulation.peer_backlog_bytes =
                response_cap.saturating_sub(1);
            prop_assert!(validate_config(&invalid).is_err());

            config.get_blocks_serving_regulation.node_outstanding_bytes =
                response_cap.saturating_sub(1);
            prop_assert!(validate_config(&config).is_err());

            let mut invalid = config.clone();
            invalid.get_blocks_serving_regulation.node_outstanding_bytes = response_cap;
            invalid.max_response_bytes = u32::try_from(block::MAX_BLOCK_BYTES)
                .expect("maximum block bytes fit u32")
                .saturating_sub(1);
            prop_assert!(validate_config(&invalid).is_err());
        }
    }

    #[test]
    fn gb_rl_14_reconnect_retains_rate_bucket_and_bounds_inactive_cache() {
        let mut config = ZakuraBlockSyncConfig::default();
        config
            .get_blocks_serving_regulation
            .peer_rate_bytes_per_second = 1;
        config
            .get_blocks_serving_regulation
            .node_rate_bytes_per_second = 1;
        let cost = serving_cost(&config, 1).expect("default request cost is valid");
        config
            .get_blocks_serving_regulation
            .peer_rate_capacity_bytes = cost.charge;
        config
            .get_blocks_serving_regulation
            .node_rate_capacity_bytes = cost.charge * 8;
        config.get_blocks_serving_regulation.peer_backlog_bytes = cost.response_cap;
        config.get_blocks_serving_regulation.node_outstanding_bytes = cost.response_cap * 8;
        let regulator = GetBlocksServingRegulator::new(config, 1);

        let peer_a = peer(0xa1);
        let session_a = regulator.session(peer_a.clone(), 1);
        let mut permit_a = session_a
            .try_admit(1)
            .expect("peer A request fits")
            .commit();
        let lease_a = permit_a.transfer_frame(GET_BLOCKS_TERMINAL_PAYLOAD_BYTES);
        drop(lease_a);
        drop(permit_a);
        let retained_a = session_a.peer_rate_balance();
        drop(session_a);

        let reconnected_a = regulator.session(peer_a.clone(), 2);
        assert_eq!(reconnected_a.peer_rate_balance(), retained_a);

        // With one inactive slot, a second identity whose smaller deficit is
        // cheaper to replenish is the one evicted. Reconnecting it can regain
        // no more than precisely that remaining deficit.
        let peer_b = peer(0xb2);
        let session_b = regulator.session(peer_b.clone(), 3);
        let capacity_b = session_b.peer_rate_capacity();
        drop(
            session_b
                .try_admit(1)
                .expect("peer B request fits")
                .commit(),
        );
        let before_eviction_b = session_b.peer_rate_balance();
        let deficit_b = capacity_b - before_eviction_b;
        // A becomes inactive while B is still active. When B follows it, the
        // cache has two inactive entries and evicts B's smaller deficit.
        drop(reconnected_a);
        drop(session_b);
        assert_eq!(regulator.snapshot().cached_peer_identities, 1);

        let reconnected_b = regulator.session(peer_b, 4);
        assert_eq!(reconnected_b.peer_rate_balance(), capacity_b);
        assert!(capacity_b - before_eviction_b <= deficit_b);

        // A live session is not part of the inactive bound and cannot be
        // evicted while unrelated identities churn.
        for byte in 0xc0..0xc4 {
            let transient = regulator.session(peer(byte), u64::from(byte));
            drop(transient);
        }
        assert_eq!(reconnected_b.peer_rate_balance(), capacity_b);
        assert!(regulator.snapshot().cached_peer_identities <= 2);

        assert_reconnect_during_drop_cannot_detach_live_bucket();
    }

    #[tokio::test]
    async fn gb_rl_18_panics_release_owned_resources_and_preserve_other_peers() {
        #[derive(Copy, Clone, Debug)]
        enum OwnershipStage {
            ProvisionalAttempt,
            CommittedPermit,
            FrameLease,
        }

        for stage in [
            OwnershipStage::ProvisionalAttempt,
            OwnershipStage::CommittedPermit,
            OwnershipStage::FrameLease,
        ] {
            let mut config = ZakuraBlockSyncConfig::default();
            config
                .get_blocks_serving_regulation
                .peer_rate_bytes_per_second = 1;
            config
                .get_blocks_serving_regulation
                .node_rate_bytes_per_second = 1;
            let regulator = GetBlocksServingRegulator::new(config, 2);
            let failed_session = regulator.session(peer(0xe1), 1);
            let node_rate_before = regulator.snapshot().node_rate_balance;
            let peer_rate_before = failed_session.peer_rate_balance();
            let panicking_session = failed_session.clone();
            let join_error = tokio::spawn(async move {
                let attempt = panicking_session
                    .try_admit(1)
                    .expect("the panic-path request fits");
                match stage {
                    OwnershipStage::ProvisionalAttempt => {
                        let _attempt = attempt;
                        panic!("panic while holding a provisional attempt");
                    }
                    OwnershipStage::CommittedPermit => {
                        let _permit = attempt.commit();
                        panic!("panic while holding a committed permit");
                    }
                    OwnershipStage::FrameLease => {
                        let mut permit = attempt.commit();
                        let _lease = permit.transfer_frame(GET_BLOCKS_TERMINAL_PAYLOAD_BYTES);
                        panic!("panic while holding a frame lease");
                    }
                }
            })
            .await
            .expect_err("the ownership task must panic");
            assert!(join_error.is_panic(), "{stage:?} must unwind its task");

            let snapshot = regulator.snapshot();
            let expected_spent = match stage {
                OwnershipStage::ProvisionalAttempt => 0,
                OwnershipStage::CommittedPermit => GET_BLOCKS_REQUEST_OVERHEAD_BYTES,
                OwnershipStage::FrameLease => {
                    GET_BLOCKS_REQUEST_OVERHEAD_BYTES + GET_BLOCKS_TERMINAL_PAYLOAD_BYTES
                }
            };
            assert_eq!(
                snapshot.node_rate_balance,
                node_rate_before - expected_spent,
                "{stage:?} must refund unused node-rate ownership"
            );
            assert_eq!(
                failed_session.peer_rate_balance(),
                peer_rate_before - expected_spent,
                "{stage:?} must refund unused peer-rate ownership"
            );
            assert_eq!(
                snapshot.node_outstanding, 0,
                "{stage:?} must release node ownership"
            );
            assert_eq!(
                failed_session.backlog_reserved(),
                0,
                "{stage:?} must release peer ownership"
            );

            let healthy_session = regulator.session(peer(0xe2), 2);
            let healthy_permit = healthy_session
                .try_admit(1)
                .unwrap_or_else(|_| panic!("{stage:?} must not block an unrelated peer"))
                .commit();
            drop(healthy_permit);
        }
    }

    fn assert_reconnect_during_drop_cannot_detach_live_bucket() {
        // Pause the old reference immediately before it locks the cache. A
        // replacement can then clone the cached account before Drop decides
        // whether that account is inactive. This deterministically exercises
        // the race that a pre-lock strong-count check would miss.
        let mut config = ZakuraBlockSyncConfig::default();
        config
            .get_blocks_serving_regulation
            .peer_rate_bytes_per_second = 1;
        config
            .get_blocks_serving_regulation
            .node_rate_bytes_per_second = 1;
        let cost = serving_cost(&config, 1).expect("default request cost is valid");
        config
            .get_blocks_serving_regulation
            .peer_rate_capacity_bytes = cost.charge;
        config
            .get_blocks_serving_regulation
            .node_rate_capacity_bytes = cost.charge;
        config.get_blocks_serving_regulation.peer_backlog_bytes = cost.response_cap;
        config.get_blocks_serving_regulation.node_outstanding_bytes = cost.response_cap;

        let regulator = GetBlocksServingRegulator::new(config, 1);
        let peer = peer(0xd0);
        let mut older = regulator.session(peer.clone(), 1);
        let hook = Arc::new(PeerRateDropHook::new());
        older.peer_account.drop_hook = Some(hook.clone());
        let dropper = std::thread::spawn(move || drop(older));

        hook.reached.wait();
        let replacement = regulator.session(peer.clone(), 2);
        hook.resume.wait();
        dropper.join().expect("the old-session drop does not panic");

        drop(
            replacement
                .try_admit(1)
                .expect("the replacement request fits")
                .commit(),
        );
        let replacement_balance = replacement.peer_rate_balance();
        assert!(replacement_balance < replacement.peer_rate_capacity());

        let observer = regulator.session(peer, 3);
        assert_eq!(
            observer.peer_rate_balance(),
            replacement_balance,
            "a live replacement must remain the cached identity account"
        );
    }
}
