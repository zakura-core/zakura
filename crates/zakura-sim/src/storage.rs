//! Storage services as action sources.
//!
//! [`HeaderStorage`] offers each pending operation of a
//! [`ControlledPort`] as `storage:header:<id>:<outcome>` actions, one per
//! outcome its contract allows now, so the `Plan` picks when and how each
//! completes. At every observation it drains the port's facts, checks them
//! and the port's actual owner state against an [`OwnerModel`], logs each
//! fact as [`Observation::Storage`] and the state as
//! [`Observation::StorageState`], and then reads the log back to confirm
//! the checker will see exactly what was drained. Any failure ends the run
//! with [`HarnessFailure::SourceEvidence`](crate::HarnessFailure).

use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use zakura_chain::block;
use zakura_header_chain::{
    BodyWorkOwner, Frontier, HeaderSyncWorkOwner, HeaderWorkAuthority, HeaderWorkOwner, SourceId,
};
use zakura_scheduler::SimInstant;
use zakura_storage::header::{
    controlled::{ControlledPort, Fact, FactKind, OpId},
    evidence::{
        CallerPresence, CapacityClass, LeaseFact, LeaseKey, OperationStage, OwnerModel, OwnerState,
        PathPosition, RequestIdentity, ResultIdentity, RetentionEnd, SignalClass, Waiter,
    },
    Fault, Outcome,
};

use crate::{
    evidence::{Checkpoint, EvidenceId, EvidenceKind, LiveEvidence},
    observe::{
        StorageBranch, StorageContinuation, StorageFact, StorageLease, StorageLeaseState,
        StorageOperation, StoragePoint, StorageRequest, StorageResult, StorageState, WorkOwner,
        WorkScope,
    },
    ActionSource, Boundary, Identity, Observation, ObservationLog, Sim,
};

/// The service name in identities and observations.
const SERVICE: &str = "header";

/// Every outcome, indexed by its code in a source-local action index.
const OUTCOMES: [Outcome; 5] = [
    Outcome::Engine,
    Outcome::Fault(Fault::Timeout),
    Outcome::Fault(Fault::Unavailable),
    Outcome::Fault(Fault::LocalResource),
    Outcome::Rejected,
];

/// Action indices hold the operation ID times this, plus the outcome code.
const CODES: u64 = 8;

/// The operation and outcome that action index `local` names.
pub(crate) fn decode(local: u64) -> (OpId, Outcome) {
    // Indices come from `enabled`, so the code is in range.
    (OpId(local / CODES), OUTCOMES[(local % CODES) as usize])
}

/// Header storage driven by the run. Dropping it shuts the port down, which
/// a run does after its log is frozen, so nothing is logged. A drop while
/// the run is live fails the run, since that shutdown is never observed.
pub struct HeaderStorage {
    port: Arc<ControlledPort>,
    max_latency: Option<Duration>,
    model: OwnerModel,
    /// The last fact delivered to the log, or `None` before the port is
    /// bound at the first observation.
    delivered: Option<u64>,
    checkpoint: Checkpoint,
    /// Binds it to the run that holds its checkpoint.
    id: EvidenceId,
    /// The service in its action identities: `header` for the run's first
    /// instance, so one-instance runs keep their identities, and
    /// `header-<index>` for each later one, so instances never share one.
    label: String,
}

impl HeaderStorage {
    /// A source for `port` with no latency bound: an operation can stay
    /// pending for as long as the `Plan` leaves it. Its checkpoint is
    /// registered with `sim` now, so the run fails unless the source
    /// observes every boundary, however it's later wrapped or added.
    ///
    /// # Panics
    ///
    /// If `port` isn't pristine, since then the model can't know its owner
    /// state. That's a fixture bug.
    pub fn new(sim: &mut Sim, port: Arc<ControlledPort>) -> Self {
        Self::with_latency(sim, port, None)
    }

    /// A source for `port` that must complete each operation within
    /// `max_latency` of its submission, for scenarios that assume bounded
    /// storage. Time stops at that deadline until the operation completes.
    ///
    /// # Panics
    ///
    /// As [`HeaderStorage::new`] does.
    pub fn bounded(sim: &mut Sim, port: Arc<ControlledPort>, max_latency: Duration) -> Self {
        Self::with_latency(sim, port, Some(max_latency))
    }

    /// The source both constructors build, with its checkpoint registered.
    fn with_latency(
        sim: &mut Sim,
        port: Arc<ControlledPort>,
        max_latency: Option<Duration>,
    ) -> Self {
        assert!(
            port.is_pristine(),
            "a storage source needs a pristine port: build it before the run and use it only \
             inside the run"
        );
        let live: Weak<dyn LiveEvidence> = Arc::<ControlledPort>::downgrade(&port);
        let checkpoint = Checkpoint::new(
            EvidenceKind::Storage {
                service: SERVICE.to_owned(),
            },
            live,
        );
        let id = sim.register(checkpoint.clone());
        HeaderStorage {
            port,
            max_latency,
            model: OwnerModel::new(),
            delivered: None,
            checkpoint,
            label: match id.index {
                0 => SERVICE.to_owned(),
                index => format!("{SERVICE}-{index}"),
            },
            id,
        }
    }
}

/// `t` plus `d`, saturating at the largest instant.
fn after(t: SimInstant, d: Duration) -> SimInstant {
    t.checked_add(d)
        .unwrap_or(SimInstant::from_start(Duration::MAX))
}

/// `bytes` as lowercase hex.
pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// `hash` as hex, in its byte order.
pub(crate) fn hash(hash: block::Hash) -> String {
    hex(&hash.0)
}

/// `peer` as hex.
fn peer(peer: SourceId) -> String {
    hex(&peer.digest())
}

/// `frontier` in plain fields.
pub(crate) fn point(frontier: Frontier) -> StoragePoint {
    StoragePoint {
        height: frontier.height.0,
        hash: hash(frontier.hash),
    }
}

/// `key` in plain fields.
fn lease(key: LeaseKey) -> StorageLease {
    StorageLease {
        peer: peer(key.peer),
        session_id: key.session_id,
        lease_id: key.lease_id,
    }
}

/// `authority` in plain fields.
pub(crate) fn scope(authority: HeaderWorkAuthority) -> WorkScope {
    WorkScope {
        header_generation: authority.header_generation.get(),
        branch: StorageBranch {
            anchor: hash(authority.branch.anchor_hash),
            target: hash(authority.branch.target_tip_hash),
        },
    }
}

/// A header owner in plain fields.
fn header_owner(owner: HeaderWorkOwner) -> WorkOwner {
    WorkOwner {
        kind: "header".to_owned(),
        scope: scope(owner.authority),
        verified_generation: None,
        body_work_epoch: None,
        session_id: owner.session_id,
        request_id: owner.request_id.get(),
    }
}

/// A body owner in plain fields.
pub(crate) fn body_owner(owner: BodyWorkOwner) -> WorkOwner {
    WorkOwner {
        kind: "body-repair".to_owned(),
        scope: scope(owner.authority.header),
        verified_generation: Some(owner.authority.verified_generation.get()),
        body_work_epoch: Some(owner.authority.body_work_epoch.get()),
        session_id: owner.session_id,
        request_id: owner.request_id.get(),
    }
}

/// A header sync owner in plain fields, by its kind.
pub fn work_owner(owner: HeaderSyncWorkOwner) -> WorkOwner {
    match owner {
        HeaderSyncWorkOwner::Header(owner) => header_owner(owner),
        HeaderSyncWorkOwner::BodyRepair(owner) => body_owner(owner),
    }
}

/// `position` as the index of the next header, or `None` once served.
fn next(position: PathPosition) -> Option<u64> {
    match position {
        PathPosition::Retained { next } => Some(next),
        PathPosition::Complete => None,
    }
}

/// `class`'s name.
fn capacity(class: CapacityClass) -> String {
    match class {
        CapacityClass::General => "general",
        CapacityClass::Bounded => "bounded",
    }
    .to_owned()
}

/// `class`'s name.
fn signal(class: SignalClass) -> String {
    match class {
        SignalClass::Peer => "peer",
        SignalClass::General => "general",
        SignalClass::Fallback => "fallback",
    }
    .to_owned()
}

/// `waiter`'s class name, and its lease for a peer's.
fn waiter(waiter: Waiter) -> (String, Option<StorageLease>) {
    match waiter {
        Waiter::Peer(key) => (signal(SignalClass::Peer), Some(lease(key))),
        Waiter::General => (signal(SignalClass::General), None),
        Waiter::Fallback => (signal(SignalClass::Fallback), None),
    }
}

/// `request` in plain fields.
pub fn request(request: &RequestIdentity) -> StorageRequest {
    match request {
        RequestIdentity::ContinuationLocator => StorageRequest::ContinuationLocator,
        RequestIdentity::VctRepairContext { owner, height } => StorageRequest::VctRepairContext {
            owner: body_owner(*owner),
            height: height.0,
        },
        RequestIdentity::AcquireHeaderPath {
            peer: source,
            session_id,
            scope: authority,
            target_tip_hash,
            locators,
        } => StorageRequest::AcquireHeaderPath {
            peer: peer(*source),
            session_id: *session_id,
            scope: scope(*authority),
            target: hash(*target_tip_hash),
            locators: locators.iter().copied().map(hash).collect(),
        },
        RequestIdentity::ReadHeaderPath {
            lease: key,
            scope: authority,
            after_hash,
            max_header_count,
        } => StorageRequest::ReadHeaderPath {
            lease: key.map(lease),
            scope: scope(*authority),
            after: hash(*after_hash),
            max_headers: *max_header_count,
        },
        RequestIdentity::ReleaseHeaderPath {
            lease: key,
            scope: authority,
        } => StorageRequest::ReleaseHeaderPath {
            lease: key.map(lease),
            scope: scope(*authority),
        },
        RequestIdentity::PrepareHeaderTarget {
            owner,
            source,
            common_ancestor,
            target,
            headers,
        } => StorageRequest::PrepareHeaderTarget {
            owner: work_owner(*owner),
            source: peer(*source),
            common_ancestor: point(*common_ancestor),
            target: point(*target),
            headers: *headers,
        },
        RequestIdentity::ApplyHeaderTarget {
            owner,
            source,
            parent_hash,
            target_tip_hash,
        } => StorageRequest::ApplyHeaderTarget {
            owner: work_owner(*owner),
            source: peer(*source),
            parent: parent_hash.map(hash),
            target: hash(*target_tip_hash),
        },
        RequestIdentity::SelectedChainContains { hash: asked } => {
            StorageRequest::SelectedChainContains { hash: hash(*asked) }
        }
    }
}

/// `result` in plain fields.
fn result(result: &ResultIdentity) -> StorageResult {
    match result {
        ResultIdentity::Fault(fault) => StorageResult::Fault {
            fault: Outcome::Fault(*fault).name().to_owned(),
        },
        ResultIdentity::Rejected => StorageResult::Rejected,
        ResultIdentity::PortError => StorageResult::PortError,
        ResultIdentity::Locator {
            tip,
            header_generation,
            finalized,
        } => StorageResult::Locator {
            tip: point(*tip),
            header_generation: header_generation.get(),
            finalized: point(*finalized),
        },
        ResultIdentity::NoLocator => StorageResult::NoLocator,
        ResultIdentity::VctResolved => StorageResult::VctResolved,
        ResultIdentity::VctStale => StorageResult::VctStale,
        ResultIdentity::Acquired {
            lease: key,
            common_ancestor,
            target,
        } => StorageResult::Acquired {
            lease: lease(*key),
            common_ancestor: point(*common_ancestor),
            target: point(*target),
        },
        ResultIdentity::CapacityBusy(class) => StorageResult::CapacityBusy {
            class: signal(*class),
        },
        ResultIdentity::Busy => StorageResult::Busy,
        ResultIdentity::TargetNotRetained => StorageResult::TargetNotRetained,
        ResultIdentity::NoLocatorIntersection => StorageResult::NoLocatorIntersection,
        ResultIdentity::HistoryPruned => StorageResult::HistoryPruned,
        ResultIdentity::Page {
            last,
            count,
            complete,
        } => StorageResult::Page {
            last: point(*last),
            count: *count,
            complete: *complete,
        },
        ResultIdentity::PageUnavailable => StorageResult::PageUnavailable,
        ResultIdentity::Released => StorageResult::Released,
        ResultIdentity::Prepared => StorageResult::Prepared,
        ResultIdentity::PrepareFailed(category) => StorageResult::PrepareFailed {
            category: category.metrics_label().to_owned(),
        },
        ResultIdentity::Applied => StorageResult::Applied,
        ResultIdentity::ResourceStalled => StorageResult::ResourceStalled,
        ResultIdentity::ApplyFailed(category) => StorageResult::ApplyFailed {
            category: category.metrics_label().to_owned(),
        },
        ResultIdentity::Selected(at) => StorageResult::Selected { at: point(*at) },
        ResultIdentity::NotSelected => StorageResult::NotSelected,
        ResultIdentity::NotRetained => StorageResult::NotRetained,
    }
}

/// A lease fact in plain fields.
fn lease_fact(fact: LeaseFact) -> StorageFact {
    match fact {
        LeaseFact::Leased {
            lease: key,
            scope: authority,
            target,
            common_ancestor,
            position,
            capacity: class,
            continued_from,
        } => StorageFact::Leased {
            lease: lease(key),
            scope: scope(authority),
            target: point(target),
            common_ancestor: point(common_ancestor),
            next: next(position),
            capacity: capacity(class),
            continued_from,
        },
        LeaseFact::CapabilityEnded {
            lease: key,
            release,
        } => StorageFact::CapabilityEnded {
            lease: lease(key),
            release: release.name().to_owned(),
        },
        LeaseFact::RetentionCached {
            lease: key,
            last_frontier,
            position,
        } => StorageFact::RetentionCached {
            lease: lease(key),
            last: point(last_frontier),
            next: next(position),
        },
        LeaseFact::RetentionEnded { lease: key, why } => {
            let (why, into) = match why {
                RetentionEnd::WithCapability => ("with-capability", None),
                RetentionEnd::Evicted => ("evicted", None),
                RetentionEnd::Continued { into } => ("continued", Some(into)),
                RetentionEnd::Discarded => ("discarded", None),
                RetentionEnd::ShutdownCleared => ("shutdown-cleared", None),
            };
            StorageFact::RetentionEnded {
                lease: lease(key),
                why: why.to_owned(),
                into,
            }
        }
        LeaseFact::WaiterSet(set) => {
            let (class, lease) = waiter(set);
            StorageFact::WaiterSet { class, lease }
        }
        LeaseFact::WaiterFired(fired) => {
            let (class, lease) = waiter(fired);
            StorageFact::WaiterFired { class, lease }
        }
    }
}

/// `fact` as storage `instance`'s observation.
fn observation(fact: Fact, instance: u64) -> Observation {
    let fact_kind = match fact.kind {
        FactKind::Submitted {
            id,
            request: asked,
            request_digest,
        } => StorageFact::Submitted {
            id: id.0,
            request: request(&asked),
            digest: hex(&request_digest),
        },
        FactKind::CallerDropped { id } => StorageFact::CallerDropped { id: id.0 },
        FactKind::ReplyTaken { id } => StorageFact::ReplyTaken { id: id.0 },
        FactKind::ReplyDropped { id } => StorageFact::ReplyDropped { id: id.0 },
        FactKind::Completed(completion) => StorageFact::Completed {
            id: completion.id.0,
            outcome: completion.outcome.name().to_owned(),
            result: result(&completion.result),
            delivered: completion.delivered,
            generation: completion.generation.get(),
            state: hex(&completion.state_digest),
        },
        FactKind::PermitTaken { id } => StorageFact::PermitTaken { id: id.0 },
        FactKind::PermitDropped { id } => StorageFact::PermitDropped { id: id.0 },
        FactKind::Lease(fact) => lease_fact(fact),
        // Mapped like any fact. Shutdown at the end of a run happens after
        // the log is frozen and is never observed.
        FactKind::ShutDown { dropped } => StorageFact::ShutDown {
            dropped: dropped.into_iter().map(|id| id.0).collect(),
        },
    };
    Observation::Storage {
        service: SERVICE.to_owned(),
        instance,
        seq: fact.seq,
        txn: fact.txn,
        fact: fact_kind,
    }
}

/// `state` in plain fields.
fn state(state: &OwnerState) -> StorageState {
    StorageState {
        operations: state
            .operations
            .iter()
            .map(|op| {
                let (stage, caller_waiting, stored) = match &op.stage {
                    OperationStage::Pending { caller_waiting } => {
                        ("pending", Some(*caller_waiting), None)
                    }
                    OperationStage::Running => ("running", None, None),
                    OperationStage::Replied { result: stored } => {
                        ("replied", None, Some(result(stored)))
                    }
                };
                StorageOperation {
                    id: op.id.0,
                    request: request(&op.request),
                    stage: stage.to_owned(),
                    caller_waiting,
                    result: stored,
                    caller_alive: op.caller == CallerPresence::Alive,
                }
            })
            .collect(),
        permits: state.permits.iter().map(|id| id.0).collect(),
        leases: state
            .leases
            .iter()
            .map(|live| StorageLeaseState {
                lease: lease(live.lease),
                scope: scope(live.scope),
                target: point(live.target),
                last: point(live.last_frontier),
                next: next(live.position),
                capacity: capacity(live.capacity),
                waiter: live.waiter,
                leased_txn: live.leased_txn,
            })
            .collect(),
        continuations: state
            .continuations
            .iter()
            .map(|cached| StorageContinuation {
                lease: lease(cached.lease),
                scope: scope(cached.scope),
                target: point(cached.target),
                last: point(cached.last_frontier),
                next: next(cached.position),
                retained: cached.retained,
            })
            .collect(),
        general_waiter: state.class_waiters.general,
        fallback_waiter: state.class_waiters.fallback,
        last_lease_id: state.last_lease_id,
        continuation_hits: state.continuation_hits,
    }
}

impl ActionSource for HeaderStorage {
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        for op in self.port.pending() {
            for outcome in op.enabled {
                let code = (0..)
                    .zip(OUTCOMES)
                    .find_map(|(code, known)| (known == outcome).then_some(code))
                    .expect("OUTCOMES lists every outcome");
                out.push((
                    Identity::storage(&self.label, op.id.0, outcome.name()),
                    op.id.0 * CODES + code,
                ));
            }
        }
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        if let Some(gate) = self.port.next_gate() {
            out.push(Boundary::Opens(after(now, gate)));
        }
        if let Some(max) = self.max_latency {
            for op in self.port.pending() {
                out.push(Boundary::Deadline(after(
                    now,
                    max.saturating_sub(op.since_submit),
                )));
            }
        }
    }

    fn execute(&mut self, local: u64, _: &mut ObservationLog) {
        let (id, outcome) = decode(local);
        self.port
            .complete(id, outcome)
            .expect("`enabled` offered this completion at this step");
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        // Only the run holding the checkpoint checks what this delivers.
        // Run IDs depend on the process, and the failure is replayed, so the
        // text names neither.
        if log.run() != self.id.run {
            return Err("ForeignRun: storage registered with another run".to_owned());
        }
        let delivered = match self.delivered {
            Some(delivered) => delivered,
            None => {
                self.port
                    .bind_to_run()
                    .map_err(|error| format!("activity before binding: {error}"))?;
                0
            }
        };
        let facts = self.port.take_facts();
        let actual = self.port.owner_state();
        self.model
            .check_boundary(&facts, &actual)
            .map_err(|failure| format!("{failure:?}"))?;
        // After setup, the state is logged only when it may have changed.
        let with_state = self.delivered.is_none() || !facts.is_empty();
        let last = facts.last().map_or(delivered, |fact| fact.seq);
        let mut sent: Vec<Observation> = facts
            .into_iter()
            .map(|fact| observation(fact, self.id.index))
            .collect();
        if with_state {
            sent.push(Observation::StorageState {
                service: SERVICE.to_owned(),
                instance: self.id.index,
                through_seq: actual.through_seq,
                state: state(&actual),
            });
        }
        for obs in &sent {
            log.push(obs.clone());
        }
        self.delivered = Some(last);
        // The run checks its own log against this, since the log this source
        // was handed may not be the run's.
        self.checkpoint.record(log.step(), &sent, last);
        Ok(())
    }
}

impl Drop for HeaderStorage {
    fn drop(&mut self) {
        // Its shutdown facts are never observed, so a run still live fails.
        self.checkpoint.dropped();
        self.port.shut_down();
    }
}
