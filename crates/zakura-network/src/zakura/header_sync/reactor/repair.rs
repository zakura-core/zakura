//! The driver for selected VCT metadata repair.

use super::*;

/// How one repair candidate's request ended.
pub(super) enum RepairAttempt {
    /// The pass is over: the request started, or the repair retried.
    Finished,
    /// The aggregate header budget had no room for the request.
    NoLocalCapacity,
    /// The request could not enter the peer's send queue. The failure is
    /// local when it describes no fault of the supplier.
    SendFailed { local: bool },
}

pub(super) fn vct_send_retry_attribution(error: &OrderedSendError) -> VctRepairRetryAttribution {
    match error {
        OrderedSendError::Full | OrderedSendError::Closed | OrderedSendError::Encode(_) => {
            VctRepairRetryAttribution::Local
        }
    }
}

pub(super) fn vct_repair_task(
    snapshot: &zakura_header_chain::EngineSnapshot,
    status: zakura_header_chain::VctRootRepairStatus,
) -> Option<RepairRequirement> {
    let zakura_header_chain::VctRootRepairState::Unavailable { height } = status.state else {
        return None;
    };
    if height <= snapshot.frontiers.finalized.height
        || height > snapshot.frontiers.header_best.height
    {
        return None;
    }
    let request_id = status.generation.checked_add(1).and_then(NonZeroU64::new)?;
    let scope = zakura_header_chain::BodyWorkAuthority::for_snapshot(snapshot);
    let owner = scope.bind(INTERNAL_VCT_REPAIR_SESSION_ID, request_id);
    Some(RepairRequirement::new(owner, height, status.generation))
}

pub(super) fn vct_repair_predecessor(
    task: &RepairRequirement,
) -> Option<zakura_header_chain::Frontier> {
    let context = match &task.state {
        RepairPolicyState::Ready { context }
        | RepairPolicyState::LocalBackoff { context, .. }
        | RepairPolicyState::StateBlocked { context, .. }
        | RepairPolicyState::Assigned { context } => context,
        RepairPolicyState::NeedsContext
        | RepairPolicyState::QueryingContext { .. }
        | RepairPolicyState::ContextBackoff { .. }
        | RepairPolicyState::Completed => return None,
    };
    context.locator.entries().first().copied()
}

impl HeaderSyncReactor {
    pub(super) fn retry_vct_repair(
        &mut self,
        owner: zakura_header_chain::BodyWorkOwner,
        retry: VctRepairRetry,
    ) {
        let source = retry.source;
        if let Some(active) = self.peer_work_queue.active_owner(owner.into()).cloned() {
            self.emit_request_terminal(&active, retry.terminal);
        }
        self.cancel_owned_request(source, owner.into());
        self.peer_work_queue.remove_owner(owner.into());
        let peer = self
            .sessions
            .iter()
            .find(|(peer, handles)| {
                handles.session.session_id() == owner.session_id
                    && source_id_from_peer(peer) == source
            })
            .map(|(peer, _)| peer.clone());
        let retry_session_is_current = peer.is_some();
        if let Some(peer) = peer {
            self.state.clear_request_deadline(&peer);
        }
        if retry.attribution == VctRepairRetryAttribution::Stale {
            if let Some(task) = self.state.vct_repair.remove(owner) {
                self.emit_vct_repair_state(&task, "terminal", Some("stale_episode"));
            }
            self.schedule_current_vct_repair();
            return;
        }
        let now = Instant::now();
        if !self
            .state
            .schedule_repair_retry(owner, retry, retry_session_is_current, now)
        {
            return;
        }
        if let Some(task) = self.state.vct_repair.current().cloned() {
            self.emit_vct_repair_state(&task, "retry", Some(retry.attribution.label()));
            if let Some(predecessor) = vct_repair_predecessor(&task) {
                self.note_vct_repair_stall(
                    &task,
                    predecessor,
                    VctSupplierRejections::default(),
                    retry.attribution.stall_outcome(),
                    now,
                );
            }
        }
        if retry.attribution.charges_supplier() {
            self.try_assign_vct_repair();
        }
    }

    /// Record the latest reason a repair generation has not completed.
    ///
    /// The reactor samples the trace row and escalates to error level once the generation has
    /// stayed stalled for [`state::VCT_REPAIR_STALL_REPORT_AFTER`]. Without the tally an
    /// operator sees only that the repair made no progress.
    pub(super) fn note_vct_repair_stall(
        &mut self,
        task: &RepairRequirement,
        predecessor: zakura_header_chain::Frontier,
        rejections: VctSupplierRejections,
        outcome: VctRepairStallOutcome,
        now: Instant,
    ) {
        metrics::counter!(
            "sync.header.vct.repair.stalled.total",
            "outcome" => outcome.label(),
            "attribution" => outcome.attribution(),
        )
        .increment(1);
        if outcome == VctRepairStallOutcome::NoEligibleSupplier {
            metrics::counter!("sync.header.vct.repair.no_supplier.total").increment(1);
        }
        self.state.record_repair_stall(
            task.repair_generation,
            predecessor,
            rejections,
            outcome,
            now,
        );
        self.refresh_vct_repair_stall(now);
    }

    pub(super) fn refresh_vct_repair_stall(&mut self, now: Instant) {
        let Some(RepairStallDue {
            task,
            record,
            trace,
            report,
            stalled_for,
            best_peer_tip,
        }) = self.state.refresh_repair_stall(now)
        else {
            return;
        };
        if trace {
            self.emit_vct_repair_stall(&task, record, best_peer_tip);
        }
        if report {
            tracing::error!(
                height = ?task.height,
                predecessor_height = ?record.predecessor.height,
                repair_generation = task.repair_generation,
                branch_target = ?task.owner.branch.target_tip_hash,
                ?best_peer_tip,
                outcome = record.outcome.label(),
                attribution = record.outcome.attribution(),
                peers_considered = record.rejections.considered,
                rejected_height = record.rejections.below_target_height,
                rejected_capacity = record.rejections.insufficient_capacity,
                rejected_schema = record.rejections.unsupported_schema,
                rejected_busy = record.rejections.already_serving,
                rejected_tried = record.rejections.already_tried,
                supplier_status_wait = record.rejections.supplier_status_wait,
                retained_source = record.rejections.retained_source,
                excluded_input = record.rejections.excluded_input,
                send_failed = record.rejections.send_failed,
                ?stalled_for,
                "VCT: repair has not completed; the parked checkpoint commit cannot advance"
            );
        }
    }

    pub(super) fn retire_vct_repair(&mut self) {
        if let Some(task) = self.state.retire_repair() {
            if let Some(peer) = self
                .peer_work_queue
                .active_owner(task.owner.into())
                .map(|active| active.peer.clone())
            {
                self.retire_peer_work(&peer, HeaderRequestTerminal::RepairObsolete);
            }
        }
    }

    pub(super) fn observe_vct_root_repair(
        &mut self,
        status: zakura_header_chain::VctRootRepairStatus,
    ) {
        if self.vct_repair_status == status {
            return;
        }
        self.vct_repair_status = status;
        self.schedule_current_vct_repair();
    }

    pub(super) fn schedule_current_vct_repair(&mut self) {
        let Some(desired) = self
            .committed_snapshot
            .as_ref()
            .and_then(|snapshot| vct_repair_task(snapshot, self.vct_repair_status))
        else {
            self.retire_vct_repair();
            return;
        };
        if self.state.preserves_repair(&desired) {
            return;
        }
        self.retire_vct_repair();
        self.state.install_repair(desired);
        metrics::counter!("sync.header.vct.repair.scheduled.total").increment(1);
        if let Some(task) = self.state.vct_repair.current() {
            self.emit_vct_repair_state(task, "schedule", None);
        }
        self.request_vct_repair_context();
    }

    pub(super) fn request_vct_repair_context(&mut self) {
        let now = Instant::now();
        if let Some(task) = self.state.vct_repair.current_mut() {
            task.resume_retry(now);
        }
        let Some(task) = self.state.vct_repair.needs_context() else {
            return;
        };
        let owner = task.owner;
        let height = task.height;
        if !self
            .dispatch_action(HeaderPortOperation::QueryVctRepairContext { owner, height })
            .is_refused()
        {
            let deadline = now + self.startup.request_timeout;
            let _ = self
                .state
                .vct_repair
                .get_mut(owner)
                .expect("the context-needing repair remains owned during synchronous dispatch")
                .mark_context_requested(deadline, deadline + VCT_REPAIR_RETRY_INTERVAL);
            if let Some(task) = self.state.vct_repair.get(owner) {
                self.emit_vct_repair_state(task, "context_request", None);
            }
        }
    }

    pub(super) fn handle_vct_repair_context_ready(
        &mut self,
        owner: zakura_header_chain::BodyWorkOwner,
        result: VctRepairContextResult,
    ) {
        if self
            .state
            .vct_repair
            .get(owner)
            .is_none_or(|task| !matches!(task.state, RepairPolicyState::QueryingContext { .. }))
        {
            return;
        }
        match result {
            VctRepairContextResult::Resolved(context) => {
                let current_state_version = self
                    .committed_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.state_version);
                if self
                    .state
                    .vct_repair
                    .get_mut(owner)
                    .expect("the exact scheduled repair was checked above")
                    .resolve(context)
                    .is_err()
                {
                    self.state.vct_repair.remove(owner);
                    return;
                }
                if let Some(current_state_version) = current_state_version {
                    self.state
                        .vct_repair
                        .get_mut(owner)
                        .expect("the resolved repair remains owned")
                        .observe_state_change(current_state_version);
                }
                if let Some(task) = self.state.vct_repair.get(owner) {
                    self.emit_vct_repair_state(task, "context_resolved", Some("resolved"));
                }
                self.request_vct_repair_context();
                self.try_assign_vct_repair();
            }
            VctRepairContextResult::Stale => {
                if let Some(task) = self.state.vct_repair.get(owner) {
                    self.emit_vct_repair_state(task, "terminal", Some("stale"));
                }
                self.state.vct_repair.remove(owner);
            }
            VctRepairContextResult::Unavailable => {
                let task = self
                    .state
                    .vct_repair
                    .get_mut(owner)
                    .expect("the exact pending context read was checked above");
                let _ = task.context_unavailable(Instant::now() + VCT_REPAIR_RETRY_INTERVAL);
                let task = task.clone();
                self.emit_vct_repair_state(&task, "retry", Some("unavailable"));
                metrics::counter!("sync.header.vct.repair.context_unavailable.total").increment(1);
            }
        }
    }

    pub(super) fn prune_vct_supplier_history(&mut self) {
        // Membership changes invalidate supplier history. Status updates do not.
        let Some(task) = self.state.vct_repair.current_mut() else {
            return;
        };
        let connected_sources: HashSet<_> = self.sessions.keys().map(source_id_from_peer).collect();
        task.retain_connected_sources(&connected_sources);
    }

    pub(super) fn try_assign_vct_repair(&mut self) {
        let now = Instant::now();
        if let Some(task) = self.state.vct_repair.current_mut() {
            task.resume_retry(now);
        }
        let Some(task) = self.state.vct_repair.ready().cloned() else {
            return;
        };
        let RepairPolicyState::Ready { context } = &task.state else {
            return;
        };
        let Some(predecessor) = context.locator.entries().first().copied() else {
            return;
        };
        let desired_count = u32::try_from(context.selected_header_count())
            .expect("a VCT repair range fits the transition header limit");
        let limits = RepairSupplierLimits {
            header_bytes: self.header_bytes,
            page_limits: self.page_limits,
            local_capacity: self
                .peer_work_queue
                .reservable_repair_header_count(desired_count),
        };
        let suppliers = self.state.vct_supplier_order.iter().filter_map(|peer| {
            let handles = self.sessions.get(peer)?;
            Some(RepairSupplierFacts {
                peer,
                status: self.state.peers.get(peer)?.last_status.as_ref()?,
                serving: self.peer_work_queue.active(peer).is_some(),
                subscription_live: handles.session.subscriber().live().is_some(),
            })
        });
        let mut pass = repair_candidates(&task, context, limits, suppliers);
        for peer in &pass.closing {
            self.close_subscription(peer);
        }
        if pass.candidates.is_empty() {
            self.note_vct_repair_stall(
                &task,
                predecessor,
                pass.rejections,
                VctRepairStallOutcome::NoEligibleSupplier,
                now,
            );
            return;
        }
        // Keep the rotating order even when a later supplier offers a larger batch.
        let mut local_capacity_unavailable = false;
        let mut local_send_failure = false;
        for candidate in pass.candidates {
            match self.try_repair_candidate(&task, context, candidate) {
                RepairAttempt::Finished => return,
                RepairAttempt::NoLocalCapacity => local_capacity_unavailable = true,
                RepairAttempt::SendFailed { local } => {
                    pass.rejections.send_failed += 1;
                    local_send_failure |= local;
                }
            }
        }
        let rejections = pass.rejections;
        if local_send_failure {
            let deferred = self
                .state
                .vct_repair
                .get_mut(task.owner)
                .is_some_and(|task| {
                    task.defer_local_retry_until(now + VCT_REPAIR_RETRY_INTERVAL)
                        .is_ok()
                });
            if deferred {
                if let Some(current) = self.state.vct_repair.get(task.owner).cloned() {
                    self.note_vct_repair_stall(
                        &current,
                        predecessor,
                        rejections,
                        VctRepairStallOutcome::LocalSendFailure,
                        now,
                    );
                }
            }
            return;
        }
        if let Some(current) = self.state.vct_repair.get(task.owner).cloned() {
            let stall_outcome = if rejections.send_failed > 0 {
                Some(VctRepairStallOutcome::SupplierSendFailure)
            } else if local_capacity_unavailable {
                Some(VctRepairStallOutcome::LocalCapacityUnavailable)
            } else {
                None
            };
            if let Some(stall_outcome) = stall_outcome {
                self.note_vct_repair_stall(&current, predecessor, rejections, stall_outcome, now);
            }
        }
        if local_capacity_unavailable {
            let _ = self.state.vct_repair.get_mut(task.owner).and_then(|task| {
                task.defer_local_retry_until(now + VCT_REPAIR_RETRY_INTERVAL)
                    .ok()
            });
        }
    }

    /// Reserve for, send, and start `task`'s repair request to `candidate`.
    pub(super) fn try_repair_candidate(
        &mut self,
        task: &RepairRequirement,
        context: &zakura_header_chain::VctRepairContext,
        candidate: RepairCandidate,
    ) -> RepairAttempt {
        let RepairCandidate {
            peer,
            source,
            mut status,
            request_count,
        } = candidate;
        if self.peer_work_queue.awaiting_target(&peer).is_some() {
            // Preemption returns the ordinary offer even if reserving or sending repair fails.
            self.defer_ordinary_target(&peer);
        }
        if self
            .peer_work_queue
            .reservable_repair_header_count(request_count)
            != request_count
            || !self
                .peer_work_queue
                .reserve_repair_request(&peer, request_count)
        {
            return RepairAttempt::NoLocalCapacity;
        }
        let session = self
            .sessions
            .get(&peer)
            .expect("a pass's candidates keep their sessions while it runs")
            .session
            .clone();
        let selected_context = context
            .bounded_prefix(
                usize::try_from(request_count).expect("the negotiated repair count fits usize"),
            )
            .expect("a positive repair prefix exists");
        let request_target = selected_context.request_target();
        status.selected_tip_height = request_target.height;
        status.selected_tip_hash = request_target.hash;
        status.max_headers_per_response = request_count;
        let request_id = match session.try_open(
            &status,
            selected_context.locator.entries(),
            AuxSchema::V1,
            request_count,
        ) {
            Ok(request_id) => request_id,
            Err(error) => {
                self.peer_work_queue.cancel_request_reservation(&peer);
                self.emit_queue_send_failed(&peer, &session, "SubscribeHeaders", &error, None);
                return match vct_send_retry_attribution(&error) {
                    VctRepairRetryAttribution::Supplier
                    | VctRepairRetryAttribution::ExcludedInput => {
                        if let Some(current) = self.state.vct_repair.get_mut(task.owner) {
                            let _ = current.record_failed_source(source);
                        }
                        self.state.rotate_supplier(source);
                        RepairAttempt::SendFailed { local: false }
                    }
                    // A full or closed outbound stream describes this one peer. Backing the
                    // whole round off here would let a peer that stops reading its stream keep
                    // its place at the front of every candidate list and fail the same send
                    // forever, so the remaining candidates still get their turn.
                    VctRepairRetryAttribution::Local => RepairAttempt::SendFailed { local: true },
                    VctRepairRetryAttribution::Stale => {
                        unreachable!("ordered-send failures cannot report a stale state episode")
                    }
                };
            }
        };
        self.emit_header_request(
            &peer,
            session.session_id(),
            task.owner.header,
            request_id,
            request_target.hash,
            &selected_context.locator,
            request_count,
            AuxSchema::V1,
        );
        let wire_owner = task.owner.authority.bind(
            session.session_id(),
            NonZeroU64::new(request_id.get()).expect("header-sync request IDs are nonzero"),
        );
        if self
            .state
            .vct_repair
            .assign(task.owner, wire_owner, selected_context.clone())
            .is_err()
        {
            self.close_subscription(&peer);
            self.peer_work_queue.cancel_request_reservation(&peer);
            return RepairAttempt::Finished;
        }
        if let Some(task) = self.state.vct_repair.get(wire_owner) {
            self.emit_vct_repair_state(task, "assignment", Some("assigned"));
        }
        let target = AdvertisedHeaderTarget {
            scope: wire_owner.header,
            session_id: session.session_id(),
            status,
        };
        if self
            .peer_work_queue
            .stage(peer.clone(), target.clone(), PeerWorkPriority::Normal)
            != QueueWorkResult::NeedsLocator
            || !self.peer_work_queue.start_repair(
                ActiveHeaderRequest::new(
                    HeaderTargetPurpose::SelectedAuxiliaryRepair {
                        selected_target: request_target,
                        repair_generation: task.repair_generation,
                    },
                    peer.clone(),
                    source,
                    selected_context.locator.clone(),
                    wire_owner.into(),
                    target,
                    request_id,
                    request_count,
                    AuxSchema::V1,
                ),
                selected_context.episode,
            )
        {
            self.close_subscription(&peer);
            self.peer_work_queue.cancel_request_reservation(&peer);
            self.retry_vct_repair(
                wire_owner,
                VctRepairRetry::local(source, HeaderRequestTerminal::LocalError),
            );
            return RepairAttempt::Finished;
        }
        observe!(
            self,
            ReactorEvent::started(
                self.peer_work_queue
                    .active(&peer)
                    .expect("the repair request was just started")
            )
        );
        self.state
            .arm_request_deadline(peer.clone(), Instant::now(), self.startup.request_timeout);
        metrics::counter!("sync.header.vct.repair.requested.total").increment(1);
        metrics::counter!("sync.header.vct.repair.requested.headers")
            .increment(u64::from(request_count));
        debug!(
            ?peer,
            start_height = context.target.height.0,
            end_height = request_target.height.0,
            header_count = request_count,
            hash = ?request_target.hash,
            "requested selected VCT metadata repair"
        );
        RepairAttempt::Finished
    }

    pub(super) fn finish_vct_local_operation(
        &mut self,
        owner: zakura_header_chain::HeaderSyncWorkOwner,
        phase: HeaderTargetPhase,
    ) {
        self.state.finish_local_operation(owner, phase);
    }

    /// Report one fatal event while retaining the original operation future until shutdown.
    pub(super) fn report_fatal_vct_local_operation(&mut self, now: Instant) -> bool {
        let operation = match self.state.expire_local_operation(now) {
            LocalOperationExpiry::Idle | LocalOperationExpiry::Running => return false,
            LocalOperationExpiry::AlreadyFatal => return true,
            LocalOperationExpiry::Expired(operation) => operation,
        };

        let elapsed = now.saturating_duration_since(operation.started_at);
        let event = HeaderSyncFatalEvent {
            phase: operation.phase_label(),
            owner: operation.owner,
            repair_generation: operation.repair_generation,
            target: operation.target,
            elapsed,
        };
        tracing::error!(
            phase = event.phase,
            owner = ?event.owner,
            repair_generation = event.repair_generation,
            branch = ?event.owner.header_authority().branch,
            target = ?event.target,
            elapsed = ?event.elapsed,
            "VCT: local repair operation exceeded its hard deadline; terminating the node"
        );
        metrics::counter!(
            "sync.header.vct.repair.local_operation_fatal.total",
            "phase" => event.phase,
        )
        .increment(1);
        if let Some(events) = self.startup.fatal_events.as_ref() {
            let _ = events.send(event);
        }
        true
    }
}
