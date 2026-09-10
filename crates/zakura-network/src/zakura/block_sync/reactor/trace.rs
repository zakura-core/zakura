use super::*;

impl BlockSyncReactor {
    pub(super) fn emit_block(
        &self,
        event: &'static str,
        build: impl FnOnce(&mut BlockTraceFields),
    ) {
        self.startup
            .trace
            .emit_event(|| BlockTraceEvent::build(event, build));
    }

    pub(super) fn trace_floor_watchdog_cancelled(
        &self,
        claim: &OutstandingClaim,
        released: super::super::work_queue::WorkReturnOutcome,
    ) {
        self.emit_block(bs_trace::BLOCK_FLOOR_WATCHDOG_CANCELLED, |row| {
            row.peer = Some(peer(&claim.peer));
            row.height = Some(super::super::trace::height(claim.height));
            row.estimated_bytes = Some(claim.meta.estimated_bytes);
            row.released_bytes = Some(released.released_bytes);
            row.returned_count = Some(released.returned_count);
            row.already_pending_count = Some(released.already_pending_count);
            row.released_count = Some(released.released_count);
            row.missing_count = Some(released.missing_count);
            row.pending_after = Some(u64::from(
                self.state.work_queue.pending_contains(claim.height),
            ));
            row.in_flight_after = Some(u64::from(
                self.state.work_queue.in_flight_contains(claim.height),
            ));
        });
    }

    /// Emit the periodic reactor snapshot used to diagnose body-sync stalls.
    ///
    /// This is the highest-signal row: a stall shows up as `body_download_floor`
    /// and `verified_block_tip` frozen while `best_header_tip` climbs, plus
    /// whichever resource is pinned (`budget_available == 0`, `applying`/`reorder`
    /// growing, or `peers_with_status == 0`).
    /// Recompute the cached download/commit throughput rates for the next trace
    /// snapshot. Called on the trace tick (not the body hot path) so the rate is
    /// measured over the inter-tick interval.
    pub(super) fn refresh_throughput(&mut self) {
        let now = Instant::now();
        // The received (download) meter is shared with the routines (they record
        // on receipt); only the reactor samples it. The committed (commit) rate is
        // sampled by the Sequencer task and read from the latest view snapshot.
        if let Ok(mut meter) = self.state.received_throughput.lock() {
            meter.sample(now);
        }
    }

    pub(super) fn trace_sync_state(&self, include_diagnostics: bool) {
        self.emit_block(bs_trace::BLOCK_SYNC_STATE, |row| {
            let floor_gap = include_diagnostics
                .then(|| self.floor_gap_diagnostics(Instant::now()))
                .flatten();
            // Skip these registry walks on per-commit view changes, where the row is
            // only needed to keep commit pipeline counters fresh.
            let slots = include_diagnostics.then(|| self.registry.slot_summary());
            let counts = include_diagnostics.then(|| self.registry.direction_status_counts());
            let peers_with_status = include_diagnostics.then(|| self.registry.peers_with_status());
            let view = self.last_view;
            let submitted_applies = view.in_flight_submission_count;
            let (received_bytes_per_sec, received_blocks_per_sec) = self
                .state
                .received_throughput
                .lock()
                .map(|meter| (meter.bytes_per_sec(), meter.blocks_per_sec()))
                .unwrap_or((0, 0));
            row.request_floor = Some(height(self.request_floor));
            row.body_download_floor = Some(height(view.download_floor));
            row.verified_block_tip = Some(height(view.verified_tip));
            row.best_header_tip = Some(height(self.state.best_header_tip));
            row.body_lag = Some(u64::from(self.body_lag()));
            row.applying = Some(view.applying_len);
            row.submitted_applies = Some(submitted_applies);
            row.reorder = Some(view.reorder_len);
            if let Some(slots) = slots {
                row.outstanding = Some(saturating_usize(slots.outstanding_requests));
            }
            if let Some(floor_gap) = floor_gap {
                row.floor_gap_height = Some(height(floor_gap.height));
                row.floor_gap_state = Some(floor_gap.state);
                row.floor_gap_servable_peers = Some(saturating_usize(floor_gap.servable_peers));
                row.floor_gap_available_peers = Some(saturating_usize(floor_gap.available_peers));
                row.floor_gap_outstanding_peers =
                    Some(saturating_usize(floor_gap.outstanding_peers));
                row.floor_gap_oldest_outstanding_ms = floor_gap.oldest_outstanding_ms;
                row.floor_gap_next_deadline_ms = floor_gap.next_deadline_ms;
            }
            row.budget_available = Some(self.state.budget.available());
            row.budget_reserved = Some(self.state.budget.reserved());
            let sequencer_input_queued_bytes = self
                .sequencer_input_bytes
                .load(std::sync::atomic::Ordering::Relaxed);
            let sequencer_input_decoded_attributed_memory_bytes = self
                .sequencer_input_decoded_attributed_memory_bytes
                .load(std::sync::atomic::Ordering::Relaxed);
            let sequencer_input_max_capacity = self.sequencer_input.max_capacity();
            let sequencer_input_capacity = self.sequencer_input.capacity();
            let sequencer_input_queued_blocks =
                sequencer_input_max_capacity.saturating_sub(sequencer_input_capacity);
            row.sequencer_input_queued_bytes = Some(sequencer_input_queued_bytes);
            row.sequencer_input_decoded_attributed_memory_bytes =
                Some(sequencer_input_decoded_attributed_memory_bytes);
            row.reorder_decoded_attributed_memory_bytes =
                Some(view.reorder_decoded_attributed_memory_bytes);
            row.applying_decoded_attributed_memory_bytes =
                Some(view.applying_decoded_attributed_memory_bytes);
            row.active_pipeline_decoded_attributed_memory_bytes = Some(
                sequencer_input_decoded_attributed_memory_bytes
                    .saturating_add(view.reorder_decoded_attributed_memory_bytes)
                    .saturating_add(view.applying_decoded_attributed_memory_bytes),
            );
            row.sequencer_input_queued_blocks =
                Some(saturating_usize(sequencer_input_queued_blocks));
            row.sequencer_input_capacity = Some(saturating_usize(sequencer_input_capacity));
            row.sequencer_input_max_capacity = Some(saturating_usize(sequencer_input_max_capacity));
            row.reorder_buffered_bytes = Some(view.reorder_buffered_bytes);
            row.applying_buffered_bytes = Some(view.applying_buffered_bytes);
            row.unsubmitted_applying_count = Some(view.unsubmitted_applying_count);
            row.in_flight_submission_bytes = Some(view.in_flight_submission_bytes);
            row.retained_pipeline_wire_bytes = Some(
                super::super::admission::RetainedPipelineBytes {
                    reorder_buffered_bytes: view.reorder_buffered_bytes,
                    applying_buffered_bytes: view.applying_buffered_bytes,
                    sequencer_input_queued_bytes,
                }
                .wire_bytes(),
            );
            row.peers = Some(saturating_usize(self.state.peers.len()));
            if let Some(peers_with_status) = peers_with_status {
                row.peers_with_status = Some(saturating_usize(peers_with_status));
            }
            // Peers that could be issued work but have no free slots are
            // saturated; the remainder want slots. If those exist and the budget
            // can't fund another worst-case block, the download path is
            // budget-limited (not peer- or work-limited) — the key throughput
            // signal toward the 1–2 Gbps target.
            if let (Some(slots), Some(peers_with_status)) = (slots, peers_with_status) {
                let peers_wanting_slots = peers_with_status.saturating_sub(slots.saturated_peers);
                let download_blocked_on_budget = u64::from(
                    peers_wanting_slots > 0
                        && self.state.budget.available() < BS_PER_BLOCK_WORST_CASE_BYTES,
                );
                row.peers_wanting_slots = Some(saturating_usize(peers_wanting_slots));
                row.download_blocked_on_budget = Some(download_blocked_on_budget);
            }
            row.received_bytes_per_sec = Some(received_bytes_per_sec);
            row.received_blocks_per_sec = Some(received_blocks_per_sec);
            row.committed_bytes_per_sec = Some(view.committed_bytes_per_sec);
            row.committed_blocks_per_sec = Some(view.committed_blocks_per_sec);
            if let Some(counts) = counts {
                row.inbound_peers = Some(saturating_usize(counts.inbound));
                row.outbound_peers = Some(saturating_usize(counts.outbound));
                row.inbound_peers_with_status = Some(saturating_usize(counts.inbound_with_status));
                row.outbound_peers_with_status =
                    Some(saturating_usize(counts.outbound_with_status));
            }
            if let Some(slots) = slots {
                row.request_slot_capacity = Some(saturating_usize(slots.capacity));
                row.request_slot_effective_window = Some(saturating_usize(slots.effective_window));
                row.request_slot_available = Some(saturating_usize(slots.available));
                row.request_slot_saturated_peers = Some(saturating_usize(slots.saturated_peers));
            }
            if include_diagnostics {
                // Scheduling visibility: distinguishes "gap not in `needed`"
                // (state/filter) from "gap in `needed` but never queued" (`ensure`
                // rejected it) from "queued but never requested" (starvation).
                if let Some(min) = self.state.needed_heights.first() {
                    row.needed_min = Some(height(*min));
                }
                row.needed_count = Some(saturating_usize(self.state.needed_heights.len()));
                row.queue_len = Some(saturating_usize(self.state.work_queue.pending_run_count()));
                row.queue_blocks = Some(saturating_usize(self.state.work_queue.pending_len()));
                if let Some(start) = self.state.work_queue.min_pending() {
                    row.queue_min_start = Some(height(start));
                }
                row.assigned_len = Some(saturating_usize(self.state.work_queue.in_flight_len()));
                row.local_body_work = Some(saturating_usize(self.local_body_work_blocks()));
                row.refill_low_water = Some(saturating_usize(self.refill_low_water_blocks()));
                if let Some(end) = self.state.work_queue.max_in_flight() {
                    row.covered_max_end = Some(height(end));
                }
            }
        });
    }

    pub(super) fn trace_status_sent(
        &self,
        peer: &ZakuraPeerId,
        reason: &'static str,
        status: BlockSyncStatus,
    ) {
        self.emit_block(bs_trace::BLOCK_STATUS_SENT, |row| {
            row.peer = Some(super::super::trace::peer(peer));
            row.reason = Some(reason);
            status_fields(row, status);
        });
    }

    pub(super) fn trace_status_send_failed(&self, peer: &ZakuraPeerId, reason: &'static str) {
        self.emit_block(bs_trace::BLOCK_STATUS_SEND_FAILED, |row| {
            row.peer = Some(super::super::trace::peer(peer));
            row.reason = Some(reason);
        });
    }

    pub(super) fn trace_queue_send_failed(
        &self,
        peer: &ZakuraPeerId,
        msg: &BlockSyncMessage,
        error: &OrderedSendError,
        queue_capacity: usize,
        queue_max_capacity: usize,
        reason: Option<&'static str>,
    ) {
        self.startup.trace.emit_event(|| {
            QueueSendFailedEvent::new(peer, msg, error, reason, queue_capacity, queue_max_capacity)
        });
    }

    pub(super) fn trace_peer_connected(
        &self,
        peer: &ZakuraPeerId,
        direction: ServicePeerDirection,
        active_connections: usize,
    ) {
        self.emit_block(bs_trace::BLOCK_PEER_CONNECTED, |row| {
            row.peer = Some(super::super::trace::peer(peer));
            row.direction = Some(direction.trace_label());
            row.active_connections = Some(saturating_usize(active_connections));
        });
    }

    pub(super) fn trace_peer_disconnected(
        &self,
        peer: &ZakuraPeerId,
        received_status: bool,
        active_connections: usize,
    ) {
        self.emit_block(bs_trace::BLOCK_PEER_DISCONNECTED, |row| {
            row.peer = Some(super::super::trace::peer(peer));
            row.received_status = Some(BoolOrU64::Bool(received_status));
            row.active_connections = Some(saturating_usize(active_connections));
        });
    }

    pub(super) fn trace_message_sent(
        &self,
        peer: &ZakuraPeerId,
        msg: &BlockSyncMessage,
        result: &'static str,
        elapsed: Duration,
    ) {
        self.emit_block(bs_trace::BLOCK_MESSAGE_SENT, |row| {
            row.peer = Some(super::super::trace::peer(peer));
            row.kind = Some(block_sync_message_label(msg));
            row.result = Some(result);
            row.elapsed_ms = Some(elapsed_ms(elapsed));
            project_message(row, msg);
        });
    }

    pub(super) fn trace_apply_finished(
        &self,
        height: block::Height,
        token: BlockApplyToken,
        result: BlockApplyResult,
        budget_reserved_after: u64,
    ) {
        self.emit_block(bs_trace::BLOCK_APPLY_FINISHED, |row| {
            row.height = Some(super::super::trace::height(height));
            row.apply_token = Some(token);
            row.result = Some(block_apply_result_label(result));
            row.budget_reserved_after = Some(budget_reserved_after);
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn trace_sequencer_control_send(
        &self,
        kind: &'static str,
        result: &'static str,
        elapsed: Duration,
        height: Option<block::Height>,
        token: Option<BlockApplyToken>,
        capacity: usize,
        max_capacity: usize,
    ) {
        self.emit_block(bs_trace::BLOCK_SEQUENCER_CONTROL_SENT, |row| {
            row.kind = Some(kind);
            row.result = Some(result);
            row.elapsed_ms = Some(elapsed_ms(elapsed));
            row.height = height.map(super::super::trace::height);
            row.apply_token = token;
            row.sequencer_input_capacity = Some(saturating_usize(capacity));
            row.sequencer_input_max_capacity = Some(saturating_usize(max_capacity));
        });
    }

    pub(super) fn trace_range_response_sent(
        &self,
        peer: &ZakuraPeerId,
        response: RangeResponseTrace,
    ) {
        self.emit_block(bs_trace::BLOCK_RANGE_RESPONSE_SENT, |row| {
            row.peer = Some(super::super::trace::peer(peer));
            row.range_start = Some(height(response.start_height));
            row.range_count = Some(u64::from(response.sent_count));
            row.expected_count = Some(u64::from(response.requested_count));
            row.serialized_bytes = Some(response.sent_bytes);
            row.reason = Some(response.reason);
            row.prepare_elapsed_ms = response.prepare_elapsed.map(elapsed_ms);
            row.send_elapsed_ms = Some(elapsed_ms(response.send_elapsed));
            row.elapsed_ms = response.total_elapsed.map(elapsed_ms);
        });
    }

    /// Trace a WorkQueue producer extend (heights newly added to `pending`).
    pub(super) fn trace_work_extended(&self, inserted: usize) {
        if !self.startup.trace.is_enabled() {
            return;
        }
        self.emit_block(bs_trace::BLOCK_WORK_EXTENDED, |row| {
            row.range_count = Some(saturating_usize(inserted));
            row.queue_blocks = Some(saturating_usize(self.state.work_queue.pending_len()));
        });
    }

    /// Trace a WorkQueue take (a contiguous chunk claimed by a peer for issuance).
    pub(super) fn trace_frontiers_changed(&self, verified_block_tip: block::Height) {
        self.emit_block(bs_trace::BLOCK_FRONTIERS_CHANGED, |row| {
            row.verified_block_tip = Some(height(verified_block_tip));
            row.best_header_tip = Some(height(self.state.best_header_tip));
        });
    }

    pub(super) fn trace_chain_tip_reset(&self, verified_block_tip: block::Height) {
        self.emit_block(bs_trace::BLOCK_CHAIN_TIP_RESET, |row| {
            row.verified_block_tip = Some(height(verified_block_tip));
        });
    }
}
