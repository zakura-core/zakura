use super::super::trace::{
    block_sync_message_label, elapsed_us, height as trace_height, peer as trace_peer,
    saturating_usize, BlockTraceEvent, BlockTraceFields, BoolOrU64, QueueSendFailedEvent,
};
use super::*;
use crate::zakura::trace::block_sync_trace as bs_trace;

impl PeerRoutine {
    pub(super) fn emit(&self, event: &'static str, build: impl FnOnce(&mut BlockTraceFields)) {
        self.trace
            .emit_event(|| BlockTraceEvent::build(event, build));
    }

    pub(super) fn trace_wake(&self, reason: &'static str) {
        self.emit("block_peer_wake", |row| {
            row.outstanding = Some(saturating_usize(self.window.outstanding.len()));
            row.reason = Some(reason);
        });
    }

    pub(super) fn trace_protocol_reject_liveness(&self, error: &str) {
        self.emit(bs_trace::BLOCK_PEER_PROTOCOL_REJECT, |row| {
            row.peer = Some(trace_peer(&self.peer));
            row.reason = Some(CLOSE_BLOCK_SYNC_NO_BLOCK_PROGRESS);
            row.error = Some(error.to_string());
            row.outstanding = Some(saturating_usize(self.window.outstanding.len()));
            row.bbr_cwnd = Some(saturating_usize(self.window.bbr_effective_cwnd()));
            row.available_slots = Some(saturating_usize(self.window.available_slots()));
            if let Some(last_block_at) = self.window.last_block_at {
                row.last_block_age_ms = Some(elapsed_ms_u64(last_block_at.elapsed()));
            }
        });
    }

    /// Trace a decoded inbound message (the previous reactor's `trace_message_received`,
    /// now emitted in the routine that decoded it). Records the message kind only;
    /// the per-variant field detail lives on the reactor's heavier trace path.
    pub(super) fn trace_message_received(&self, msg: &BlockSyncMessage) {
        self.emit(bs_trace::BLOCK_MESSAGE_RECEIVED, |row| {
            row.kind = Some(block_sync_message_label(msg));
        });
    }

    pub(super) fn trace_status_received(&self, status: BlockSyncStatus) {
        self.emit(bs_trace::BLOCK_STATUS_RECEIVED, |row| {
            row.peer = Some(trace_peer(&self.peer));
            row.servable_low = Some(trace_height(status.servable_low));
            row.servable_high = Some(trace_height(status.servable_high));
        });
    }

    pub(super) fn trace_work_taken(&self, low: block::Height, high: block::Height, count: usize) {
        self.emit(bs_trace::BLOCK_WORK_TAKEN, |row| {
            row.servable_low = Some(trace_height(low));
            row.servable_high = Some(trace_height(high));
            row.range_count = Some(saturating_usize(count));
        });
    }

    pub(super) fn trace_work_returned(
        &self,
        reason: &'static str,
        outstanding: &OutstandingBlockRange,
        unreceived_count: usize,
        outcome: WorkReturnOutcome,
    ) {
        let unreceived_count = u64::try_from(unreceived_count).unwrap_or(u64::MAX);
        if outcome.missing_count == 0
            && outcome.released_count == 0
            && outcome.returned_count == unreceived_count
        {
            return;
        }

        self.emit(bs_trace::BLOCK_WORK_RETURNED, |row| {
            row.peer = Some(trace_peer(&self.peer));
            row.reason = Some(reason);
            row.range_start = Some(trace_height(outstanding.request.start_height));
            row.range_count = Some(u64::from(outstanding.request.count));
            row.unreceived_count = Some(unreceived_count);
            insert_work_return_outcome(row, outcome);
        });
    }

    /// Emitted when a `try_fill` pass issued no request (a candidate carrier "bubble").
    /// The reason plus the live slot/budget/work snapshot let a trace tell a legitimate
    /// idle (`no_work` with an empty queue, `cwnd_saturated`) from a recoverable one
    /// (slots + budget + work all free yet stopped — a wakeup gap to fix).
    pub(super) fn trace_fill_stop(&self, reason: &'static str) {
        self.emit(bs_trace::BLOCK_FILL_STOP, |row| {
            // Mirror the effective (reliability-scaled) bypass the fill loop used.
            let base_floor_bonus = usize::try_from(self.config.floor_bypass_slots).unwrap_or(0);
            let floor_bonus = self.window.scaled_floor_bonus(base_floor_bonus);
            let now = Instant::now();
            row.peer = Some(trace_peer(&self.peer));
            row.fill_stop_reason = Some(reason);
            row.fill_sent = Some(0);
            row.normal_slots = Some(saturating_usize(self.window.available_slots_at(now)));
            row.floor_slots = Some(saturating_usize(
                self.window.available_slots_with_bonus_at(floor_bonus, now),
            ));
            row.budget_available = Some(self.budget.available());
            row.pending_work = Some(saturating_usize(self.work.pending_len()));
            row.received_status = Some(BoolOrU64::U64(u64::from(self.received_status)));
        });
    }

    pub(super) fn trace_queue_send_failed(&self, msg: &BlockSyncMessage, error: &OrderedSendError) {
        self.trace.emit_event(|| {
            QueueSendFailedEvent::peer_routine(
                &self.peer,
                msg,
                error,
                self.session.outbound_capacity(),
                self.session.outbound_max_capacity(),
            )
        });
    }

    pub(super) fn trace_get_blocks_sent(
        &self,
        start_height: block::Height,
        count: u32,
        estimated_bytes: u64,
        floor_bypass: bool,
    ) {
        self.emit(bs_trace::BLOCK_GET_BLOCKS_SENT, |row| {
            row.peer = Some(trace_peer(&self.peer));
            row.range_start = Some(trace_height(start_height));
            row.range_count = Some(u64::from(count));
            row.estimated_bytes = Some(estimated_bytes);
            row.available_slots = Some(saturating_usize(self.window.available_slots()));
            row.peer_outstanding = Some(saturating_usize(self.window.outstanding.len()));
            self.insert_no_progress_fields(row);
            // The reliability estimate discounts the admission cwnd, so trace it at
            // request time too (not only on delivery): a dropping peer keeps requesting at
            // a shrinking cwnd, and these rows capture the fall.
            row.bbr_reliability_permille = Some(self.window.bbr_reliability_permille());
            // A floor request issued while the peer was saturated at its cwnd — borrowed
            // a floor-bypass slot. Lets the analysis confirm the bypass actually fired.
            row.floor_bypass = Some(u64::from(floor_bypass));
        });
    }

    pub(super) fn trace_body_received(
        &self,
        height: block::Height,
        serialized_bytes: u64,
        decoded_attributed_memory_size_bytes: u64,
        request_start_height: Option<block::Height>,
        request_range_count: Option<u32>,
        request_elapsed_ms: Option<u64>,
    ) {
        self.emit(bs_trace::BLOCK_BODY_RECEIVED, |row| {
            row.peer = Some(trace_peer(&self.peer));
            row.height = Some(trace_height(height));
            row.serialized_bytes = Some(serialized_bytes);
            row.decoded_attributed_memory_size_bytes = Some(decoded_attributed_memory_size_bytes);
            row.budget_reserved_after = Some(self.budget.reserved());
            row.sequencer_input_capacity = Some(saturating_usize(self.sequencer_input.capacity()));
            row.sequencer_input_max_capacity =
                Some(saturating_usize(self.sequencer_input.max_capacity()));
            if let Some(request_start_height) = request_start_height {
                row.request_start = Some(trace_height(request_start_height));
            }
            if let Some(request_range_count) = request_range_count {
                row.request_range_count = Some(u64::from(request_range_count));
            }
            if let Some(request_elapsed_ms) = request_elapsed_ms {
                row.request_elapsed_ms = Some(request_elapsed_ms);
            }
            self.insert_bbr_fields(row);
        });
    }

    /// Insert the per-peer BBR controller fields (effective cwnd, RTprop, BtlBw, phase,
    /// delay-gradient ceiling, reliability) into a trace row. Shared by the per-delivery
    /// `block_body_received` row and the `block_peer_bbr` heartbeat so both report an
    /// identical field set.
    /// Insert the per-peer no-progress accounting fields shared by the GetBlocks-sent row
    /// and the BBR heartbeat, so the two row types stay in lockstep — one definition of the
    /// field names and their `u64` encoding, rather than a copy that can drift stylistically.
    pub(super) fn insert_no_progress_fields(&self, row: &mut BlockTraceFields) {
        row.requests_without_block_progress =
            Some(u64::from(self.window.requests_without_block_progress));
        row.no_progress_request_cap = Some(u64::from(self.window.no_progress_request_cap()));
        row.block_progress_proven = Some(u64::from(self.window.has_block_progress()));
    }

    pub(super) fn insert_bbr_fields(&self, row: &mut BlockTraceFields) {
        // Read the windowed estimators as of now, so a trace taken during a quiet bad
        // period reports freshly-filtered (possibly `None`) values, not stale ones.
        let now = Instant::now();
        row.bbr_cwnd = Some(saturating_usize(self.window.bbr_effective_cwnd()));
        if let Some(rtprop_ms) = self.window.bbr_rtprop_ms(now) {
            row.bbr_rtprop_ms = Some(rtprop_ms);
        }
        if let Some(btlbw) = self.window.bbr_btlbw_milliblocks(now) {
            row.bbr_btlbw_milliblocks_per_sec = Some(btlbw);
        }
        // Byte-denomination fields (emitted only under `CwndUnit::Bytes`): byte cwnd,
        // bytes/sec BtlBw, in-flight reserved bytes. `bbr_cwnd` above stays the derived
        // in-flight *request* count so existing analysis scripts work in either unit.
        if let Some(cwnd_bytes) = self.window.bbr_effective_cwnd_bytes() {
            row.bbr_cwnd_bytes = Some(cwnd_bytes);
            row.bbr_inflight_bytes = Some(self.window.bbr_inflight_bytes());
        }
        if let Some(btlbw_bytes) = self.window.bbr_btlbw_bytes_per_sec(now) {
            row.bbr_btlbw_bytes_per_sec = Some(btlbw_bytes);
        }
        row.bbr_delivered = Some(self.window.bbr_delivered());
        row.bbr_phase = Some(self.window.bbr_phase_code());
        if let Some(smoothed_ms) = self.window.bbr_smoothed_elapsed_ms() {
            row.bbr_smoothed_elapsed_ms = Some(smoothed_ms);
        }
        if let Some(delay_cap) = self.window.bbr_delay_cap() {
            row.bbr_delay_cap = Some(delay_cap);
        }
        row.bbr_reliability_permille = Some(self.window.bbr_reliability_permille());
    }

    /// Emit the periodic per-peer BBR heartbeat (`block_peer_bbr`). Fires even while the
    /// peer is idle, so the controller's balance is observable between deliveries — e.g.
    /// a cwnd that keeps ramping up only to be pulled back by the reliability discount
    /// instead of settling near `r = 1.0`.
    pub(super) fn trace_bbr_sample(&self) {
        self.emit(bs_trace::BLOCK_PEER_BBR, |row| {
            row.peer = Some(trace_peer(&self.peer));
            row.peer_outstanding = Some(saturating_usize(self.window.outstanding.len()));
            row.budget_reserved = Some(self.budget.reserved());
            self.insert_no_progress_fields(row);
            self.insert_bbr_fields(row);
        });
        // Refresh the published slot diagnostics on the same cadence so the cross-peer
        // floor-preference view cannot hold a stale-low RTprop for a quiet peer:
        // `publish_outstanding` re-reads `bbr_rtprop_ms(now)`, filtering out samples aged
        // past the horizon (→ `None` = worst floor server).
        self.publish_outstanding();
    }

    pub(super) fn trace_body_sequencer_sent(
        &self,
        height: block::Height,
        elapsed: Duration,
        ok: bool,
    ) {
        self.emit(bs_trace::BLOCK_BODY_SEQUENCER_SENT, |row| {
            row.peer = Some(trace_peer(&self.peer));
            row.height = Some(trace_height(height));
            row.sequencer_send_elapsed_us = Some(elapsed_us(elapsed));
            row.ok = Some(ok);
        });
    }

    pub(super) fn trace_body_decode_permit(&self, elapsed: Duration, capacity_before: usize) {
        self.emit(bs_trace::BLOCK_BODY_DECODE_PERMIT, |row| {
            row.peer = Some(trace_peer(&self.peer));
            row.decode_permit_wait_us = Some(elapsed_us(elapsed));
            row.sequencer_input_capacity_before = Some(saturating_usize(capacity_before));
            row.sequencer_input_max_capacity =
                Some(saturating_usize(self.sequencer_input.max_capacity()));
        });
    }

    pub(super) fn trace_range_unavailable(
        &self,
        start_height: block::Height,
        range_count: Option<u32>,
        request_elapsed_ms: Option<u64>,
    ) {
        self.emit(bs_trace::BLOCK_RANGE_UNAVAILABLE, |row| {
            row.peer = Some(trace_peer(&self.peer));
            row.range_start = Some(trace_height(start_height));
            if let Some(range_count) = range_count {
                row.range_count = Some(u64::from(range_count));
            }
            if let Some(request_elapsed_ms) = request_elapsed_ms {
                row.request_elapsed_ms = Some(request_elapsed_ms);
            }
        });
    }
}

fn insert_work_return_outcome(row: &mut BlockTraceFields, outcome: WorkReturnOutcome) {
    row.released_bytes = Some(outcome.released_bytes);
    row.returned_count = Some(outcome.returned_count);
    row.already_pending_count = Some(outcome.already_pending_count);
    row.released_count = Some(outcome.released_count);
    row.missing_count = Some(outcome.missing_count);
    if let Some(height) = outcome.min_height {
        row.return_min_height = Some(trace_height(height));
    }
    if let Some(height) = outcome.max_height {
        row.return_max_height = Some(trace_height(height));
    }
}
