use super::super::trace::{height as trace_height, saturating_usize, BlockTraceEvent};
use super::{state::FrontierResetClassified, *};
use crate::zakura::trace::block_sync_trace as bs_trace;

impl SequencerIO {
    pub(super) fn trace_body_submitted(&self, height: block::Height, token: BlockApplyToken) {
        self.trace.emit_event(|| {
            BlockTraceEvent::build(bs_trace::BLOCK_BODY_SUBMITTED, |row| {
                row.height = Some(trace_height(height));
                row.apply_token = Some(token);
            })
        });
    }

    pub(super) fn trace_submission_retry_scheduled(&self, height: block::Height) {
        self.trace.emit_event(|| {
            BlockTraceEvent::build(bs_trace::BLOCK_BODY_SUBMISSION_RETRY_SCHEDULED, |row| {
                row.height = Some(trace_height(height));
                row.queue_capacity = Some(saturating_usize(self.actions.capacity()));
                row.queue_max_capacity = Some(saturating_usize(self.actions.max_capacity()));
                row.in_flight_submission_count = Some(saturating_usize(
                    self.state.sequencer().in_flight_submission_count(),
                ));
                row.unsubmitted_applying_count = Some(saturating_usize(
                    self.state.sequencer().unsubmitted_applying_count(),
                ));
                row.retry_attempt = Some(self.state.submission_retry_attempt());
            })
        });
    }

    pub(super) fn trace_body_accepted(
        &self,
        height: block::Height,
        queued_elapsed: Duration,
        outcome: &'static str,
    ) {
        self.trace.emit_event(|| {
            BlockTraceEvent::build(bs_trace::BLOCK_BODY_ACCEPTED, |row| {
                row.height = Some(trace_height(height));
                row.sequencer_queue_elapsed_us =
                    Some(super::super::trace::elapsed_us(queued_elapsed));
                row.result = Some(outcome);
            })
        });
    }

    pub(super) fn trace_frontier_reset_classified(&self, classified: &FrontierResetClassified) {
        self.trace.emit_event(|| {
            BlockTraceEvent::build(bs_trace::BLOCK_FRONTIER_RESET_CLASSIFIED, |row| {
                row.result = Some(classified.kind.label());
                row.verified_block_tip =
                    Some(trace_height(classified.frontiers.verified_block_tip));
                row.previous_verified_tip = Some(trace_height(classified.previous_verified_tip));
                row.previous_download_floor =
                    Some(trace_height(classified.previous_download_floor));
                row.preserve_active_successors =
                    Some(u64::from(classified.preserve_active_successors));
                row.peer_has_successor_after = Some(u64::from(classified.peer_has_successor_after));
                row.peer_outstanding_conflicts_at_tip =
                    Some(u64::from(classified.peer_outstanding_conflicts_at_tip));
                row.reset_tip_matches_local_work =
                    Some(u64::from(classified.reset_tip_matches_local_work));
                row.has_local_successor_after =
                    Some(u64::from(classified.has_local_successor_after));
            })
        });
    }
}
