//! Trace rows and their labels for the header-sync reactor.

use super::*;

impl HeaderSyncReactor {
    pub(super) fn emit_target_outcome(
        &self,
        event: &'static str,
        stage: &'static str,
        peer: &ZakuraPeerId,
        owner: zakura_header_chain::HeaderSyncWorkOwner,
        error: Option<&zakura_header_chain::HeaderChainError>,
    ) {
        let direction = self
            .sessions
            .get(peer)
            .map(|handles| header_direction_label(handles.session.direction()));
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(hs_trace::EVENT.into(), event.into());
            row.insert(hs_trace::PEER.into(), trace_peer_label(peer).into());
            row.insert(hs_trace::SESSION_ID.into(), owner.session_id().into());
            row.insert(
                hs_trace::DIRECTION.into(),
                direction.map_or(serde_json::Value::Null, Into::into),
            );
            insert_header_scope(row, owner.header_authority());
            row.insert(hs_trace::REQUEST_ID.into(), owner.request_id().get().into());
            row.insert(hs_trace::STAGE.into(), stage.into());
            row.insert(
                hs_trace::CATEGORY.into(),
                error.map_or(serde_json::Value::Null, |error| {
                    error.category.metrics_label().into()
                }),
            );
            row.insert(
                hs_trace::ATTRIBUTION.into(),
                error.map_or(serde_json::Value::Null, |error| {
                    error.attribution.metrics_label().into()
                }),
            );
        });
    }

    pub(super) fn emit_vct_repair_state(
        &self,
        task: &RepairRequirement,
        phase: &'static str,
        outcome: Option<&'static str>,
    ) {
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            insert_vct_repair_identity(row, task, phase);
            row.insert(
                hs_trace::OUTCOME.into(),
                outcome.map_or(serde_json::Value::Null, Into::into),
            );
        });
    }

    pub(super) fn emit_vct_repair_stall(
        &self,
        task: &RepairRequirement,
        record: VctRepairStall,
        best_peer_tip: Option<(block::Height, block::Hash)>,
    ) {
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            insert_vct_repair_identity(
                row,
                task,
                if record.outcome == VctRepairStallOutcome::NoEligibleSupplier {
                    "no_supplier"
                } else {
                    "stall"
                },
            );
            row.insert(
                hs_trace::PREDECESSOR_HEIGHT.into(),
                u64::from(record.predecessor.height.0).into(),
            );
            row.insert(
                hs_trace::PEERS_CONSIDERED.into(),
                record.rejections.considered.into(),
            );
            row.insert(
                hs_trace::REJECTED_HEIGHT.into(),
                record.rejections.below_target_height.into(),
            );
            row.insert(
                hs_trace::REJECTED_CAPACITY.into(),
                record.rejections.insufficient_capacity.into(),
            );
            row.insert(
                hs_trace::REJECTED_SCHEMA.into(),
                record.rejections.unsupported_schema.into(),
            );
            row.insert(
                hs_trace::REJECTED_BUSY.into(),
                record.rejections.already_serving.into(),
            );
            row.insert(
                hs_trace::REJECTED_TRIED.into(),
                record.rejections.already_tried.into(),
            );
            row.insert(
                "supplier_status_wait".into(),
                record.rejections.supplier_status_wait.into(),
            );
            row.insert(
                hs_trace::BEST_PEER_HEIGHT.into(),
                best_peer_tip.map_or(serde_json::Value::Null, |(height, _)| {
                    u64::from(height.0).into()
                }),
            );
            row.insert(
                hs_trace::BEST_PEER_HASH.into(),
                best_peer_tip.map_or(serde_json::Value::Null, |(_, hash)| hash.to_string().into()),
            );
            row.insert(hs_trace::OUTCOME.into(), record.outcome.label().into());
            row.insert(
                hs_trace::ATTRIBUTION.into(),
                record.outcome.attribution().into(),
            );
        });
    }

    pub(super) fn emit_snapshot_observed(
        &self,
        old: Option<&zakura_header_chain::EngineSnapshot>,
        new: &zakura_header_chain::EngineSnapshot,
    ) {
        let cause = match old {
            None => "startup",
            Some(old) if old.frontiers.finalized.hash != new.frontiers.finalized.hash => "reanchor",
            Some(old) if new.frontiers.header_best.height > old.frontiers.header_best.height => {
                "advance"
            }
            Some(_) => "refresh",
        };
        if cause == "refresh" {
            let now = Instant::now();
            if !snapshot_refresh_trace_due(self.last_snapshot_refresh_trace_at.get(), now) {
                return;
            }
            self.last_snapshot_refresh_trace_at.set(Some(now));
        }
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(
                hs_trace::EVENT.into(),
                hs_trace::HEADER_SNAPSHOT_OBSERVED.into(),
            );
            row.insert(hs_trace::CAUSE.into(), cause.into());
            row.insert(
                hs_trace::STATE_VERSION.into(),
                new.state_version.get().into(),
            );
            row.insert(
                hs_trace::HEADER_GENERATION.into(),
                new.header_generation.get().into(),
            );
            row.insert(
                hs_trace::VERIFIED_GENERATION.into(),
                new.verified_generation.get().into(),
            );
            row.insert(
                hs_trace::BRANCH_ANCHOR.into(),
                new.frontiers.finalized.hash.to_string().into(),
            );
            row.insert(
                hs_trace::BRANCH_TARGET.into(),
                new.frontiers.header_best.hash.to_string().into(),
            );
            row.insert(
                hs_trace::OLD_SELECTED_HEIGHT.into(),
                old.map_or(serde_json::Value::Null, |old| {
                    u64::from(old.frontiers.header_best.height.0).into()
                }),
            );
            row.insert(
                hs_trace::OLD_SELECTED_HASH.into(),
                old.map_or(serde_json::Value::Null, |old| {
                    old.frontiers.header_best.hash.to_string().into()
                }),
            );
            row.insert(
                hs_trace::NEW_SELECTED_HEIGHT.into(),
                u64::from(new.frontiers.header_best.height.0).into(),
            );
            row.insert(
                hs_trace::NEW_SELECTED_HASH.into(),
                new.frontiers.header_best.hash.to_string().into(),
            );
        });
    }

    pub(super) fn emit_peer_lifecycle(
        &self,
        event: &'static str,
        peer: &ZakuraPeerId,
        session_id: u64,
        direction: ServicePeerDirection,
        reason: Option<&'static str>,
    ) {
        let inbound = self.admitted_count(ServicePeerDirection::Inbound);
        let outbound = self.admitted_count(ServicePeerDirection::Outbound);
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(hs_trace::EVENT.into(), event.into());
            row.insert(hs_trace::PEER.into(), trace_peer_label(peer).into());
            row.insert(hs_trace::SESSION_ID.into(), session_id.into());
            row.insert(
                hs_trace::DIRECTION.into(),
                header_direction_label(direction).into(),
            );
            row.insert(hs_trace::INBOUND_COUNT.into(), inbound.into());
            row.insert(hs_trace::OUTBOUND_COUNT.into(), outbound.into());
            row.insert(
                hs_trace::REASON.into(),
                reason.map_or(serde_json::Value::Null, Into::into),
            );
        });
    }

    pub(super) fn emit_status(
        &self,
        event: &'static str,
        peer: &ZakuraPeerId,
        session_id: u64,
        status: &Status,
    ) {
        let direction = self
            .sessions
            .get(peer)
            .map(|handles| header_direction_label(handles.session.direction()));
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(hs_trace::EVENT.into(), event.into());
            row.insert(hs_trace::PEER.into(), trace_peer_label(peer).into());
            row.insert(hs_trace::SESSION_ID.into(), session_id.into());
            row.insert(
                hs_trace::DIRECTION.into(),
                direction.map_or(serde_json::Value::Null, Into::into),
            );
            row.insert(
                hs_trace::WORK_ANCHOR_HEIGHT.into(),
                u64::from(status.work_anchor_height.0).into(),
            );
            row.insert(
                hs_trace::WORK_ANCHOR_HASH.into(),
                status.work_anchor_hash.to_string().into(),
            );
            row.insert(
                hs_trace::SELECTED_TIP_HEIGHT.into(),
                u64::from(status.selected_tip_height.0).into(),
            );
            row.insert(
                hs_trace::SELECTED_TIP_HASH.into(),
                status.selected_tip_hash.to_string().into(),
            );
            row.insert(
                hs_trace::MAX_HEADERS_PER_RESPONSE.into(),
                u64::from(status.max_headers_per_response).into(),
            );
            row.insert(
                hs_trace::MAX_SUBSCRIPTIONS.into(),
                u64::from(status.max_subscriptions).into(),
            );
            row.insert(
                hs_trace::MAX_MESSAGE_BYTES.into(),
                u64::from(status.max_message_bytes).into(),
            );
            row.insert(
                hs_trace::TREE_AUX_SCHEMA_MASK.into(),
                u64::from(status.tree_aux_schema_mask).into(),
            );
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn emit_header_request(
        &self,
        peer: &ZakuraPeerId,
        session_id: u64,
        scope: zakura_header_chain::HeaderWorkAuthority,
        request_id: HeaderSyncRequestId,
        target_hash: block::Hash,
        locator: &zakura_header_chain::HeaderLocator,
        max_header_count: u32,
        tree_aux_schema: AuxSchema,
    ) {
        let direction = self
            .sessions
            .get(peer)
            .map(|handles| header_direction_label(handles.session.direction()));
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(hs_trace::EVENT.into(), hs_trace::HEADER_REQUEST_SENT.into());
            row.insert(hs_trace::PEER.into(), trace_peer_label(peer).into());
            row.insert(hs_trace::SESSION_ID.into(), session_id.into());
            row.insert(
                hs_trace::STREAM_VERSION.into(),
                u64::from(ZAKURA_HEADER_SYNC_STREAM_VERSION).into(),
            );
            row.insert(
                hs_trace::DIRECTION.into(),
                direction.map_or(serde_json::Value::Null, Into::into),
            );
            insert_header_scope(row, scope);
            row.insert(hs_trace::REQUEST_ID.into(), request_id.get().into());
            row.insert(hs_trace::TARGET_HASH.into(), target_hash.to_string().into());
            row.insert(
                hs_trace::LOCATOR_COUNT.into(),
                u64::try_from(locator.entries().len())
                    .unwrap_or(u64::MAX)
                    .into(),
            );
            row.insert(
                hs_trace::LOCATOR_HEAD.into(),
                locator
                    .entries()
                    .first()
                    .map_or(serde_json::Value::Null, |entry| {
                        entry.hash.to_string().into()
                    }),
            );
            row.insert(
                hs_trace::HEADER_COUNT.into(),
                u64::from(max_header_count).into(),
            );
            row.insert(
                hs_trace::TREE_AUX_SCHEMA.into(),
                aux_schema_label(tree_aux_schema).into(),
            );
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn emit_header_response(
        &self,
        event: &'static str,
        peer: &ZakuraPeerId,
        session_id: u64,
        scope: zakura_header_chain::HeaderWorkAuthority,
        request_id: u64,
        target_hash: block::Hash,
        common_ancestor_height: block::Height,
        common_ancestor_hash: block::Hash,
        header_count: usize,
        complete: bool,
        tree_aux_schema: AuxSchema,
    ) {
        let direction = self
            .sessions
            .get(peer)
            .map(|handles| header_direction_label(handles.session.direction()));
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(hs_trace::EVENT.into(), event.into());
            row.insert(hs_trace::PEER.into(), trace_peer_label(peer).into());
            row.insert(hs_trace::SESSION_ID.into(), session_id.into());
            row.insert(
                hs_trace::DIRECTION.into(),
                direction.map_or(serde_json::Value::Null, Into::into),
            );
            insert_header_scope(row, scope);
            row.insert(hs_trace::REQUEST_ID.into(), request_id.into());
            row.insert(hs_trace::TARGET_HASH.into(), target_hash.to_string().into());
            row.insert(
                hs_trace::COMMON_ANCESTOR_HEIGHT.into(),
                u64::from(common_ancestor_height.0).into(),
            );
            row.insert(
                hs_trace::COMMON_ANCESTOR_HASH.into(),
                common_ancestor_hash.to_string().into(),
            );
            row.insert(
                hs_trace::HEADER_COUNT.into(),
                u64::try_from(header_count).unwrap_or(u64::MAX).into(),
            );
            row.insert(hs_trace::COMPLETE.into(), complete.into());
            row.insert(
                hs_trace::TREE_AUX_SCHEMA.into(),
                aux_schema_label(tree_aux_schema).into(),
            );
        });
    }

    pub(super) fn emit_queue_send_failed(
        &self,
        peer: &ZakuraPeerId,
        session: &PeerSession,
        message: &'static str,
        error: &OrderedSendError,
        request_id: Option<u64>,
    ) {
        self.startup.trace.emit_with(QUEUE_SEND_TABLE, |row| {
            row.insert(qs_trace::EVENT.into(), qs_trace::QUEUE_SEND_FAILED.into());
            row.insert(qs_trace::SERVICE.into(), "header_sync".into());
            row.insert(qs_trace::MESSAGE.into(), message.into());
            row.insert(qs_trace::PEER.into(), trace_peer_label(peer).into());
            row.insert(qs_trace::SESSION_ID.into(), session.session_id().into());
            row.insert(
                qs_trace::ERROR.into(),
                ordered_send_error_label(error).into(),
            );
            row.insert(
                qs_trace::QUEUE_CAPACITY.into(),
                u64::try_from(session.outbound_capacity())
                    .unwrap_or(u64::MAX)
                    .into(),
            );
            row.insert(
                qs_trace::QUEUE_MAX_CAPACITY.into(),
                u64::try_from(session.outbound_max_capacity())
                    .unwrap_or(u64::MAX)
                    .into(),
            );
            row.insert(
                qs_trace::REQUEST_ID.into(),
                request_id.map_or(serde_json::Value::Null, Into::into),
            );
        });
    }

    pub(super) fn emit_request_terminal(
        &self,
        active: &ActiveHeaderRequest,
        terminal_outcome: HeaderRequestTerminal,
    ) {
        observe!(self, ReactorEvent::retired(active, terminal_outcome));
        if !terminal_outcome.needs_terminal_trace() {
            return;
        }
        let direction = self.sessions.get(&active.peer).and_then(|handles| {
            (handles.session.session_id() == active.owner.session_id())
                .then(|| header_direction_label(handles.session.direction()))
        });
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(
                hs_trace::EVENT.into(),
                hs_trace::HEADER_REQUEST_TERMINAL.into(),
            );
            row.insert(hs_trace::PEER.into(), trace_peer_label(&active.peer).into());
            row.insert(
                hs_trace::SESSION_ID.into(),
                active.owner.session_id().into(),
            );
            row.insert(
                hs_trace::DIRECTION.into(),
                direction.map_or(serde_json::Value::Null, Into::into),
            );
            insert_header_scope(row, active.owner.header_authority());
            row.insert(hs_trace::REQUEST_ID.into(), active.request_id.get().into());
            row.insert(
                hs_trace::TARGET_HASH.into(),
                active.target.status.selected_tip_hash.to_string().into(),
            );
            row.insert(hs_trace::OUTCOME.into(), terminal_outcome.label().into());
        });
    }
}

pub(super) fn header_direction_label(direction: ServicePeerDirection) -> &'static str {
    match direction {
        ServicePeerDirection::Inbound => "inbound",
        ServicePeerDirection::Outbound => "outbound",
    }
}

pub(super) fn aux_schema_label(schema: AuxSchema) -> &'static str {
    match schema {
        AuxSchema::None => "none",
        AuxSchema::V1 => "v1",
    }
}

/// Insert the fields every `header_vct_repair_state` row carries.
///
/// The event has two variants — the per-phase progress row and the no-supplier tally — and this
/// keeps them from drifting apart in schema.
pub(super) fn insert_vct_repair_identity(
    row: &mut serde_json::Map<String, serde_json::Value>,
    task: &RepairRequirement,
    phase: &'static str,
) {
    row.insert(
        hs_trace::EVENT.into(),
        hs_trace::HEADER_VCT_REPAIR_STATE.into(),
    );
    insert_header_scope(row, task.owner.header);
    row.insert(hs_trace::SESSION_ID.into(), task.owner.session_id.into());
    row.insert(
        hs_trace::REQUEST_ID.into(),
        task.owner.request_id.get().into(),
    );
    row.insert(hs_trace::HEIGHT.into(), u64::from(task.height.0).into());
    row.insert(
        hs_trace::REPAIR_GENERATION.into(),
        task.repair_generation.into(),
    );
    row.insert(hs_trace::PHASE.into(), phase.into());
    row.insert(
        hs_trace::SUPPLIER_COUNT.into(),
        u64::try_from(task.tried_sources.len())
            .unwrap_or(u64::MAX)
            .into(),
    );
}

pub(super) fn insert_header_scope(
    row: &mut serde_json::Map<String, serde_json::Value>,
    scope: zakura_header_chain::HeaderWorkAuthority,
) {
    row.insert(hs_trace::STATE_VERSION.into(), serde_json::Value::Null);
    row.insert(
        hs_trace::HEADER_GENERATION.into(),
        scope.header_generation.get().into(),
    );
    row.insert(
        hs_trace::VERIFIED_GENERATION.into(),
        serde_json::Value::Null,
    );
    row.insert(
        hs_trace::BRANCH_ANCHOR.into(),
        scope.branch.anchor_hash.to_string().into(),
    );
    row.insert(
        hs_trace::BRANCH_TARGET.into(),
        scope.branch.target_tip_hash.to_string().into(),
    );
}

pub(super) fn ordered_send_error_label(error: &OrderedSendError) -> &'static str {
    match error {
        OrderedSendError::Full => "full",
        OrderedSendError::Closed => "closed",
        OrderedSendError::Encode(_) => "encode",
    }
}
