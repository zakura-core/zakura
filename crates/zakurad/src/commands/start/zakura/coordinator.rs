use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use tokio::sync::{watch, Notify};
use zakura_node_services::sync_lifecycle::{
    ApplyPhase, ApplyTransition, HeaderRuntimeDetachedReason, HeaderRuntimeStatus, LifecycleEpoch,
    LifecycleTransitionError, SyncServiceDemand,
};

/// Sole process-local owner of bulk block-apply lifecycle transitions.
#[derive(Debug)]
pub(crate) struct SyncCoordinator {
    phase: Mutex<ApplyPhase>,
    phase_tx: watch::Sender<ApplyPhase>,
    header_status: Mutex<HeaderRuntimeStatus>,
    service_demand_tx: watch::Sender<SyncServiceDemand>,
    in_flight: std::sync::atomic::AtomicUsize,
    next_operation_id: std::sync::atomic::AtomicU64,
    operations: Mutex<BTreeMap<BlockApplyOperationId, BlockApplyOperationRecord>>,
    drained: Notify,
    phase_changed: Notify,
}

/// Tracks one in-flight native block apply.
#[derive(Debug)]
pub(crate) struct BlockApplyPermit(Arc<SyncCoordinator>);

/// Stable process-local identity for one native block-apply operation.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct BlockApplyOperationId(u64);

/// Observable lifecycle of one queued or accepted native apply.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum BlockApplyOperationState {
    Queued,
    CancelRequested,
    Accepted,
    TooLate,
    Cancelled,
    TransferredToLegacy,
    Committed,
    Rejected,
    Failed,
}

#[derive(Debug)]
struct BlockApplyOperationRecord {
    state: BlockApplyOperationState,
    state_tx: watch::Sender<BlockApplyOperationState>,
}

/// The driver owns this queued operation.
/// Dropping the operation acknowledges queued cancellation.
#[derive(Debug)]
pub(crate) struct BlockApplyOperation {
    coordinator: Arc<SyncCoordinator>,
    id: BlockApplyOperationId,
    finished: bool,
}

/// Accepted operation that owns the native apply permit through terminal completion.
#[derive(Debug)]
pub(crate) struct AcceptedBlockApplyOperation {
    coordinator: Arc<SyncCoordinator>,
    id: BlockApplyOperationId,
    permit: Option<BlockApplyPermit>,
    finished: bool,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum BlockApplyTerminal {
    Committed,
    Rejected,
    TransferredToLegacy,
}

/// Authorization for one legacy fallback round after native admission stops.
///
/// Full semantic commits drain before this lease activates. The block driver transfers incomplete
/// checkpoint ranges to the shared checkpoint verifier so fallback can supply their missing bodies.
#[derive(Debug)]
pub(crate) struct LegacyFallbackLease {
    coordinator: Arc<SyncCoordinator>,
    epoch: LifecycleEpoch,
}

impl SyncCoordinator {
    /// Start with native Zakura sync authorized to apply bodies.
    pub(crate) fn new() -> Arc<Self> {
        Self::new_with_phase(ApplyPhase::Native {
            epoch: LifecycleEpoch::INITIAL,
        })
    }

    /// Start with the legacy-compatible genesis fetch authorized until native handoff.
    pub(crate) fn new_legacy_bootstrap() -> Arc<Self> {
        Self::new_with_phase(ApplyPhase::LegacyBootstrap {
            epoch: LifecycleEpoch::INITIAL,
        })
    }

    fn new_with_phase(phase: ApplyPhase) -> Arc<Self> {
        let header_status = HeaderRuntimeStatus::Detached {
            epoch: LifecycleEpoch::INITIAL,
            reason: HeaderRuntimeDetachedReason::AwaitingSemanticHandoff,
        };
        let (phase_tx, _phase_rx) = watch::channel(phase);
        let (service_demand_tx, _service_demand_rx) =
            watch::channel(SyncServiceDemand::from_phases(phase, &header_status));
        let coordinator = Arc::new(Self {
            phase: Mutex::new(phase),
            phase_tx,
            header_status: Mutex::new(header_status),
            service_demand_tx,
            in_flight: std::sync::atomic::AtomicUsize::new(0),
            next_operation_id: std::sync::atomic::AtomicU64::new(0),
            operations: Mutex::new(BTreeMap::new()),
            drained: Notify::new(),
            phase_changed: Notify::new(),
        });
        coordinator.publish_phase(phase);
        coordinator
    }

    /// Subscribe to the one typed capability and ordered-service demand publication.
    pub(crate) fn subscribe_service_demand(&self) -> watch::Receiver<SyncServiceDemand> {
        self.service_demand_tx.subscribe()
    }

    /// Subscribe to authoritative apply-phase changes for driver wakeups.
    pub(crate) fn subscribe_apply_phase(&self) -> watch::Receiver<ApplyPhase> {
        self.phase_tx.subscribe()
    }

    /// Observe state-owned header attachment without letting consumers infer readiness.
    pub(crate) fn observe_header_runtime(
        &self,
        observed: &HeaderRuntimeStatus,
    ) -> Result<bool, LifecycleTransitionError> {
        let mut current = self
            .header_status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *current == *observed {
            return Ok(false);
        }
        validate_header_observation(&current, observed)?;
        *current = observed.clone();
        drop(current);
        self.publish_service_demand();
        Ok(true)
    }

    /// Return the current authoritative apply phase.
    pub(crate) fn apply_phase(&self) -> ApplyPhase {
        *self.lock_phase()
    }

    /// Whether fallback is draining or has acquired exclusive legacy authorization.
    pub(crate) fn is_yielded_to_legacy(&self) -> bool {
        matches!(
            self.apply_phase(),
            ApplyPhase::FallbackDraining { .. } | ApplyPhase::LegacyFallback { .. }
        )
    }

    /// Whether native Zakura sync currently owns block applies.
    pub(crate) fn zakura_owns_applies(&self) -> bool {
        matches!(self.apply_phase(), ApplyPhase::Native { .. })
    }

    /// Transfers initial block-apply ownership to native Zakura exactly once.
    pub(crate) fn finish_legacy_bootstrap(&self) -> Result<(), LifecycleTransitionError> {
        self.transition(ApplyTransition::FinishBootstrap)
            .map(|_| ())
    }

    /// Wait until bootstrap completes or fallback owns/drains the apply pipeline.
    pub(crate) async fn wait_for_zakura_ownership(&self) {
        loop {
            let changed = self.phase_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if matches!(
                self.apply_phase(),
                ApplyPhase::Native { .. }
                    | ApplyPhase::FallbackDraining { .. }
                    | ApplyPhase::LegacyFallback { .. }
                    | ApplyPhase::Failed { .. }
            ) {
                return;
            }
            changed.await;
        }
    }

    /// Wait until fallback starts draining native applies.
    pub(crate) async fn wait_for_legacy_yield(&self) {
        loop {
            let changed = self.phase_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_yielded_to_legacy() {
                return;
            }
            changed.await;
        }
    }

    /// Reserve one apply in the exact current native epoch.
    #[cfg(test)]
    pub(crate) fn begin_apply(self: &Arc<Self>) -> Option<BlockApplyPermit> {
        let initial = self.apply_phase();
        if !matches!(initial, ApplyPhase::Native { .. }) {
            return None;
        }

        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Reserve before rechecking the complete phase and epoch.
        // Concurrent fallback either observes this permit in the drain count or changes the phase.
        // A phase change makes this reservation reject and release itself here.
        if self.apply_phase() != initial {
            self.release_apply();
            return None;
        }

        Some(BlockApplyPermit(self.clone()))
    }

    /// Register one queued apply in the exact current native epoch.
    pub(crate) fn queue_apply(self: &Arc<Self>) -> Option<BlockApplyOperation> {
        let phase = self.lock_phase();
        if !matches!(*phase, ApplyPhase::Native { .. }) {
            return None;
        }
        let raw_id = self
            .next_operation_id
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |id| id.checked_add(1),
            )
            .ok()?;
        let id = BlockApplyOperationId(raw_id);
        let (state_tx, _state_rx) = watch::channel(BlockApplyOperationState::Queued);
        self.operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                id,
                BlockApplyOperationRecord {
                    state: BlockApplyOperationState::Queued,
                    state_tx,
                },
            );
        drop(phase);
        metrics::gauge!("sync.zakura.apply.operations").set(self.operation_count() as f64);
        Some(BlockApplyOperation {
            coordinator: self.clone(),
            id,
            finished: false,
        })
    }

    /// Request the same acknowledged operation quiescence used by fallback during shutdown.
    pub(crate) fn request_apply_shutdown(&self) {
        self.request_operation_cancellation();
    }

    fn release_apply(&self) {
        if self
            .in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            == 1
        {
            self.drained.notify_waiters();
        }
    }

    /// Stop native admission, quiesce the exact epoch, then authorize one legacy round.
    pub(crate) async fn acquire_legacy_fallback(
        self: &Arc<Self>,
        diagnostic_interval: Duration,
    ) -> Result<LegacyFallbackLease, LifecycleTransitionError> {
        let ApplyPhase::Native { epoch } = self.apply_phase() else {
            return Err(LifecycleTransitionError::IllegalPhase);
        };
        self.transition(ApplyTransition::BeginFallback {
            expected_epoch: epoch,
        })?;
        self.request_operation_cancellation();
        let lease = LegacyFallbackLease {
            coordinator: self.clone(),
            epoch,
        };
        self.wait_for_applies(diagnostic_interval).await;
        self.transition(ApplyTransition::ActivateFallback {
            expected_epoch: epoch,
        })?;
        metrics::gauge!("sync.zakura.legacy_fallback.active").set(1.0);
        Ok(lease)
    }

    async fn wait_for_applies(&self, diagnostic_interval: Duration) {
        let diagnostic_interval = if diagnostic_interval.is_zero() {
            Duration::from_secs(1)
        } else {
            diagnostic_interval
        };
        let started = tokio::time::Instant::now();
        loop {
            let drained = self.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            let in_flight = self.in_flight.load(std::sync::atomic::Ordering::SeqCst);
            let operations = self.operation_count();
            if in_flight == 0 && operations == 0 {
                return;
            }
            if tokio::time::timeout(diagnostic_interval, drained)
                .await
                .is_err()
            {
                tracing::warn!(
                    in_flight,
                    operations,
                    apply_epoch = self.apply_phase().epoch().get(),
                    elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "native block applies remain active while fallback waits for its exclusive lease"
                );
            }
        }
    }

    fn request_operation_cancellation(&self) {
        let mut operations = self
            .operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for record in operations.values_mut() {
            let next = match record.state {
                BlockApplyOperationState::Queued => BlockApplyOperationState::CancelRequested,
                BlockApplyOperationState::Accepted => BlockApplyOperationState::TooLate,
                state => state,
            };
            if next != record.state {
                record.state = next;
                record.state_tx.send_replace(next);
            }
        }
        drop(operations);
        self.phase_changed.notify_waiters();
    }

    fn accept_operation(
        self: &Arc<Self>,
        id: BlockApplyOperationId,
    ) -> Option<AcceptedBlockApplyOperation> {
        let phase = self.lock_phase();
        let mut operations = self
            .operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let accepted = operations.get_mut(&id).is_some_and(|record| {
            matches!(*phase, ApplyPhase::Native { .. })
                && record.state == BlockApplyOperationState::Queued
        });
        if !accepted {
            if let Some(record) = operations.get_mut(&id) {
                record.state = BlockApplyOperationState::Cancelled;
                record
                    .state_tx
                    .send_replace(BlockApplyOperationState::Cancelled);
            }
            operations.remove(&id);
            drop(operations);
            drop(phase);
            self.drained.notify_waiters();
            metrics::gauge!("sync.zakura.apply.operations").set(self.operation_count() as f64);
            return None;
        }
        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let Some(record) = operations.get_mut(&id) else {
            self.release_apply();
            return None;
        };
        record.state = BlockApplyOperationState::Accepted;
        record
            .state_tx
            .send_replace(BlockApplyOperationState::Accepted);
        drop(operations);
        drop(phase);
        Some(AcceptedBlockApplyOperation {
            coordinator: self.clone(),
            id,
            permit: Some(BlockApplyPermit(self.clone())),
            finished: false,
        })
    }

    fn cancel_operation(&self, id: BlockApplyOperationId) {
        let mut operations = self
            .operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(record) = operations.get_mut(&id) {
            if matches!(
                record.state,
                BlockApplyOperationState::Queued | BlockApplyOperationState::CancelRequested
            ) {
                record.state = BlockApplyOperationState::Cancelled;
                record
                    .state_tx
                    .send_replace(BlockApplyOperationState::Cancelled);
                operations.remove(&id);
            }
        }
        drop(operations);
        self.drained.notify_waiters();
        metrics::gauge!("sync.zakura.apply.operations").set(self.operation_count() as f64);
    }

    fn finish_operation(&self, id: BlockApplyOperationId, terminal: BlockApplyTerminal) {
        let mut operations = self
            .operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(record) = operations.get_mut(&id) {
            let state = match terminal {
                BlockApplyTerminal::Committed => BlockApplyOperationState::Committed,
                BlockApplyTerminal::Rejected => BlockApplyOperationState::Rejected,
                BlockApplyTerminal::TransferredToLegacy => {
                    BlockApplyOperationState::TransferredToLegacy
                }
            };
            record.state = state;
            record.state_tx.send_replace(state);
            operations.remove(&id);
        }
        drop(operations);
        self.drained.notify_waiters();
        metrics::gauge!("sync.zakura.apply.operations").set(self.operation_count() as f64);
    }

    fn fail_operation(&self, id: BlockApplyOperationId) {
        let failed = {
            let mut phase = self.lock_phase();
            let failed = ApplyPhase::Failed {
                epoch: phase.epoch(),
            };
            let changed = *phase != failed;
            *phase = failed;
            drop(phase);
            changed.then_some(failed)
        };
        if let Some(failed) = failed {
            self.publish_phase(failed);
        }

        let mut operations = self
            .operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(record) = operations.get_mut(&id) {
            record.state = BlockApplyOperationState::Failed;
            record
                .state_tx
                .send_replace(BlockApplyOperationState::Failed);
            operations.remove(&id);
        }
        drop(operations);
        self.drained.notify_waiters();
        metrics::gauge!("sync.zakura.apply.operations").set(self.operation_count() as f64);
    }

    fn operation_count(&self) -> usize {
        self.operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    fn transition(
        &self,
        transition: ApplyTransition,
    ) -> Result<ApplyPhase, LifecycleTransitionError> {
        let mut phase = self.lock_phase();
        let previous = *phase;
        let next = match previous.transition(transition) {
            Ok(next) => next,
            Err(LifecycleTransitionError::EpochExhausted) => {
                let failed = ApplyPhase::Failed {
                    epoch: previous.epoch(),
                };
                *phase = failed;
                drop(phase);
                self.publish_phase(failed);
                return Err(LifecycleTransitionError::EpochExhausted);
            }
            Err(error) => return Err(error),
        };
        *phase = next;
        drop(phase);
        self.publish_phase(next);
        Ok(next)
    }

    fn lock_phase(&self) -> MutexGuard<'_, ApplyPhase> {
        self.phase.lock().unwrap_or_else(|poisoned| {
            let mut phase = poisoned.into_inner();
            *phase = ApplyPhase::Failed {
                epoch: phase.epoch(),
            };
            phase
        })
    }

    fn publish_phase(&self, phase: ApplyPhase) {
        self.phase_tx.send_replace(phase);
        self.phase_changed.notify_waiters();
        self.publish_service_demand();
        // This diagnostic gauge may round epochs above the exact `f64` integer range.
        // Lifecycle authority always uses the original checked `u64` value.
        metrics::gauge!("sync.zakura.apply.epoch").set(phase.epoch().get() as f64);
        metrics::gauge!("sync.zakura.apply.phase").set(match phase {
            ApplyPhase::LegacyBootstrap { .. } => 0.0,
            ApplyPhase::Native { .. } => 1.0,
            ApplyPhase::FallbackDraining { .. } => 2.0,
            ApplyPhase::LegacyFallback { .. } => 3.0,
            ApplyPhase::Failed { .. } => 4.0,
        });
        tracing::info!(
            apply_phase = phase.label(),
            apply_epoch = phase.epoch().get(),
            "sync apply lifecycle changed"
        );
    }

    fn publish_service_demand(&self) {
        let apply = self.apply_phase();
        let header = self
            .header_status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let demand = SyncServiceDemand::from_phases(apply, &header);
        self.service_demand_tx.send_replace(demand);
        metrics::gauge!("sync.zakura.service.header_enabled")
            .set(f64::from(demand.header.is_enabled()));
        metrics::gauge!("sync.zakura.service.block_applying")
            .set(f64::from(demand.block.is_applying()));
        tracing::debug!(?demand, "sync ordered-service demand changed");
    }
}

fn validate_header_observation(
    current: &HeaderRuntimeStatus,
    observed: &HeaderRuntimeStatus,
) -> Result<(), LifecycleTransitionError> {
    if observed.epoch() < current.epoch() {
        return Err(LifecycleTransitionError::StaleEpoch {
            expected: observed.epoch(),
            current: current.epoch(),
        });
    }

    if observed.epoch() > current.epoch() {
        if current.epoch().checked_next() != Some(observed.epoch())
            || !matches!(current, HeaderRuntimeStatus::Detached { .. })
            || matches!(observed, HeaderRuntimeStatus::Detached { .. })
        {
            return Err(LifecycleTransitionError::IllegalPhase);
        }
        return Ok(());
    }

    let allowed = matches!(
        (current, observed),
        (
            HeaderRuntimeStatus::Reconstructing { .. },
            HeaderRuntimeStatus::Reconstructing { .. }
                | HeaderRuntimeStatus::Ready { .. }
                | HeaderRuntimeStatus::Failed { .. }
        ) | (
            HeaderRuntimeStatus::Detached { .. },
            HeaderRuntimeStatus::Failed { .. }
        ) | (
            HeaderRuntimeStatus::Ready { .. },
            HeaderRuntimeStatus::Ready { .. }
        ) | (
            HeaderRuntimeStatus::Failed { .. },
            HeaderRuntimeStatus::Failed { .. }
        )
    );
    allowed
        .then_some(())
        .ok_or(LifecycleTransitionError::IllegalPhase)
}

impl Drop for BlockApplyPermit {
    fn drop(&mut self) {
        self.0.release_apply();
    }
}

impl BlockApplyOperation {
    pub(crate) const fn id(&self) -> BlockApplyOperationId {
        self.id
    }

    #[cfg(test)]
    pub(crate) fn subscribe(&self) -> watch::Receiver<BlockApplyOperationState> {
        self.coordinator
            .operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&self.id)
            .expect("a live queued operation remains registered")
            .state_tx
            .subscribe()
    }

    pub(crate) fn accept(mut self) -> Option<AcceptedBlockApplyOperation> {
        let accepted = self.coordinator.accept_operation(self.id);
        self.finished = true;
        accepted
    }

    pub(crate) fn cancel(mut self) {
        self.coordinator.cancel_operation(self.id);
        self.finished = true;
    }
}

impl Drop for BlockApplyOperation {
    fn drop(&mut self) {
        if !self.finished {
            self.coordinator.cancel_operation(self.id);
        }
    }
}

impl AcceptedBlockApplyOperation {
    pub(crate) const fn id(&self) -> BlockApplyOperationId {
        self.id
    }

    pub(crate) fn complete(mut self, terminal: BlockApplyTerminal) {
        drop(self.permit.take());
        self.coordinator.finish_operation(self.id, terminal);
        self.finished = true;
    }
}

impl Drop for AcceptedBlockApplyOperation {
    fn drop(&mut self) {
        if !self.finished {
            tracing::error!(
                operation_id = self.id.0,
                "accepted block apply lost terminal observation; apply lifecycle is failed"
            );
            self.coordinator.fail_operation(self.id);
            drop(self.permit.take());
            self.finished = true;
        }
    }
}

impl Drop for LegacyFallbackLease {
    fn drop(&mut self) {
        if let Err(error) = self.coordinator.transition(ApplyTransition::ResumeNative {
            expected_epoch: self.epoch,
        }) {
            tracing::error!(
                ?error,
                fallback_epoch = self.epoch.get(),
                current_phase = ?self.coordinator.apply_phase(),
                "legacy fallback lease could not restore native apply ownership"
            );
        }
        metrics::gauge!("sync.zakura.legacy_fallback.active").set(0.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stale_fallback_lease_cannot_change_a_new_epoch() {
        let coordinator = SyncCoordinator::new();
        let first = coordinator
            .acquire_legacy_fallback(Duration::from_millis(1))
            .await
            .expect("the initial native epoch drains");
        assert!(matches!(
            coordinator.apply_phase(),
            ApplyPhase::LegacyFallback {
                epoch: LifecycleEpoch::INITIAL
            }
        ));
        drop(first);
        let resumed = coordinator.apply_phase();
        assert!(matches!(resumed, ApplyPhase::Native { .. }));

        let stale = LegacyFallbackLease {
            coordinator: coordinator.clone(),
            epoch: LifecycleEpoch::INITIAL,
        };
        drop(stale);
        assert_eq!(coordinator.apply_phase(), resumed);
    }

    #[tokio::test]
    async fn phase_receiver_observes_bootstrap_and_fallback_epochs() {
        let coordinator = SyncCoordinator::new_legacy_bootstrap();
        let mut phases = coordinator.phase_tx.subscribe();
        coordinator
            .finish_legacy_bootstrap()
            .expect("bootstrap advances to native");
        phases
            .changed()
            .await
            .expect("coordinator publisher is live");
        let native = *phases.borrow_and_update();
        let ApplyPhase::Native { epoch } = native else {
            panic!("bootstrap publishes native ownership");
        };
        let lease = coordinator
            .acquire_legacy_fallback(Duration::from_millis(1))
            .await
            .expect("native ownership drains");
        assert!(matches!(
            *phases.borrow(),
            ApplyPhase::LegacyFallback { epoch: current } if current == epoch
        ));
        drop(lease);
        assert!(matches!(
            *phases.borrow(),
            ApplyPhase::Native { epoch: current } if current == epoch.checked_next().expect("test epoch advances")
        ));
    }

    #[tokio::test]
    async fn one_demand_stream_tracks_header_capability_and_apply_epochs() {
        use zakura_node_services::sync_lifecycle::{BlockServiceDemand, HeaderServiceDemand};

        let coordinator = SyncCoordinator::new_legacy_bootstrap();
        let mut demand = coordinator.subscribe_service_demand();
        assert!(matches!(
            demand.borrow_and_update().header,
            HeaderServiceDemand::Disabled { .. }
        ));
        assert!(matches!(
            demand.borrow().block,
            BlockServiceDemand::ServingOnly { .. }
        ));

        coordinator
            .observe_header_runtime(&HeaderRuntimeStatus::Ready {
                epoch: LifecycleEpoch::new(1),
            })
            .expect("state readiness advances the coordinator header epoch");
        demand
            .changed()
            .await
            .expect("the coordinator demand publisher remains live");
        assert!(matches!(
            demand.borrow_and_update().header,
            HeaderServiceDemand::Enabled {
                capability_epoch
            } if capability_epoch == LifecycleEpoch::new(1)
        ));
        assert!(matches!(
            demand.borrow().block,
            BlockServiceDemand::ServingOnly { .. }
        ));

        coordinator
            .finish_legacy_bootstrap()
            .expect("bootstrap hands apply ownership to native sync");
        demand.changed().await.expect("native demand is published");
        assert!(matches!(
            demand.borrow_and_update().block,
            BlockServiceDemand::ServingAndApplying { .. }
        ));

        let lease = coordinator
            .acquire_legacy_fallback(Duration::from_millis(1))
            .await
            .expect("the empty native apply epoch drains");
        assert!(matches!(
            demand.borrow().block,
            BlockServiceDemand::ServingOnly { .. }
        ));
        assert!(demand.borrow().header.is_enabled());
        drop(lease);
        assert!(matches!(
            demand.borrow().block,
            BlockServiceDemand::ServingAndApplying { .. }
        ));
    }

    #[test]
    fn stale_header_observation_cannot_disable_current_demand() {
        let coordinator = SyncCoordinator::new();
        coordinator
            .observe_header_runtime(&HeaderRuntimeStatus::Ready {
                epoch: LifecycleEpoch::new(1),
            })
            .expect("the first readiness epoch is current");
        let stale = HeaderRuntimeStatus::Reconstructing {
            epoch: LifecycleEpoch::INITIAL,
            progress: zakura_node_services::sync_lifecycle::HeaderReconstructionProgress::STARTING,
        };
        assert!(matches!(
            coordinator.observe_header_runtime(&stale),
            Err(LifecycleTransitionError::StaleEpoch { .. })
        ));
        assert!(coordinator
            .subscribe_service_demand()
            .borrow()
            .header
            .is_enabled());
    }

    #[tokio::test]
    async fn queued_operation_cancels_before_fallback_activation() {
        let coordinator = SyncCoordinator::new();
        let operation = coordinator
            .queue_apply()
            .expect("native ownership admits a queued operation");
        let mut state = operation.subscribe();
        let fallback_coordinator = coordinator.clone();
        let fallback = tokio::spawn(async move {
            fallback_coordinator
                .acquire_legacy_fallback(Duration::from_millis(1))
                .await
                .expect("fallback activates after acknowledged cancellation")
        });
        state
            .changed()
            .await
            .expect("fallback publishes a cancellation request");
        assert_eq!(*state.borrow(), BlockApplyOperationState::CancelRequested);
        operation.cancel();
        state
            .changed()
            .await
            .expect("terminal cancellation is observable before publisher teardown");
        assert_eq!(*state.borrow(), BlockApplyOperationState::Cancelled);
        let lease = fallback.await.expect("the fallback task remains live");
        assert_eq!(coordinator.operation_count(), 0);
        drop(lease);
    }

    #[tokio::test]
    async fn accepted_operation_is_too_late_then_terminal_before_fallback() {
        let coordinator = SyncCoordinator::new();
        let operation = coordinator
            .queue_apply()
            .expect("native ownership admits a queued operation");
        let mut state = operation.subscribe();
        let accepted = operation
            .accept()
            .expect("the queued operation becomes accepted");
        state.changed().await.expect("acceptance is observable");
        assert_eq!(*state.borrow(), BlockApplyOperationState::Accepted);
        let fallback_coordinator = coordinator.clone();
        let fallback = tokio::spawn(async move {
            fallback_coordinator
                .acquire_legacy_fallback(Duration::from_millis(1))
                .await
                .expect("fallback activates after terminal completion")
        });
        state
            .changed()
            .await
            .expect("fallback publishes that cancellation is too late");
        assert_eq!(*state.borrow(), BlockApplyOperationState::TooLate);
        assert!(!fallback.is_finished());
        accepted.complete(BlockApplyTerminal::Committed);
        state
            .changed()
            .await
            .expect("terminal completion is observable before publisher teardown");
        assert_eq!(*state.borrow(), BlockApplyOperationState::Committed);
        let lease = fallback.await.expect("the fallback task remains live");
        assert_eq!(coordinator.operation_count(), 0);
        drop(lease);
    }

    #[tokio::test]
    async fn dropped_accepted_operation_fails_instead_of_blocking_fallback() {
        let coordinator = SyncCoordinator::new();
        let operation = coordinator
            .queue_apply()
            .expect("native ownership admits a queued operation");
        let mut state = operation.subscribe();
        let accepted = operation
            .accept()
            .expect("the queued operation becomes accepted");
        state.changed().await.expect("acceptance is observable");

        let fallback_coordinator = coordinator.clone();
        let fallback = tokio::spawn(async move {
            fallback_coordinator
                .acquire_legacy_fallback(Duration::from_millis(1))
                .await
        });
        state
            .changed()
            .await
            .expect("fallback publishes that cancellation is too late");
        assert_eq!(*state.borrow(), BlockApplyOperationState::TooLate);

        drop(accepted);
        assert_eq!(*state.borrow(), BlockApplyOperationState::Failed);
        let result = tokio::time::timeout(Duration::from_secs(1), fallback)
            .await
            .expect("an abandoned accepted apply must not strand fallback")
            .expect("the fallback task remains live");
        assert!(matches!(
            result,
            Err(LifecycleTransitionError::IllegalPhase)
        ));
        assert!(matches!(
            coordinator.apply_phase(),
            ApplyPhase::Failed { .. }
        ));
        assert_eq!(coordinator.operation_count(), 0);
        assert_eq!(
            coordinator
                .in_flight
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn dropping_an_observer_does_not_release_an_accepted_operation() {
        let coordinator = SyncCoordinator::new();
        let operation = coordinator
            .queue_apply()
            .expect("native ownership admits a queued operation");
        drop(operation.subscribe());
        let accepted = operation
            .accept()
            .expect("caller observation is independent of worker acceptance");
        assert_eq!(coordinator.operation_count(), 1);
        accepted.complete(BlockApplyTerminal::Rejected);
        assert_eq!(coordinator.operation_count(), 0);
    }
}
