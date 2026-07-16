//! Invariant checks over a finished fuzzer run.
//!
//! These turn a scenario from a sim into a fuzzer: a violation fails the test (and,
//! with `ZAKURA_TEST_TRACE=keep`, persists the trace for the analysis scripts). The
//! strongest correctness signal is "reached target", because the mock commit pipeline
//! (`MockApplyFrontier`) only advances on an in-order, hash-correct body — so reaching
//! the target proves every height committed exactly once, contiguously, with the
//! corpus hash. The trace-derived bounds catch download-side regressions.

use serde_json::Value;
use std::collections::HashMap;

use super::scenario::{FuzzOutcome, Scenario};
use crate::zakura::testkit::TraceReader;

/// Aggregate facts extracted from one run's `block_sync` trace table.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct InvariantReport {
    /// Number of `block_sync_state` snapshots emitted (tracing liveness).
    pub(crate) state_samples: usize,
    /// Peak aggregate in-flight requests across all peers.
    pub(crate) max_outstanding: u64,
    /// Peak reserved outstanding-request bytes (wire-limiter pressure; received
    /// bodies release their reservation, so this excludes retained bodies).
    pub(crate) peak_budget_reserved: u64,
    /// Peak retained pipeline wire bytes, retained as a diagnostic.
    pub(crate) peak_retained_pipeline_wire_bytes: u64,
    /// Peak authoritative retained body memory.
    pub(crate) peak_retained_memory_bytes: u64,
    /// Authoritative retained body memory after reactor and harness teardown.
    pub(crate) final_retained_memory_bytes: u64,
    /// Peak retained body memory above the configured soft cap.
    pub(crate) peak_retained_memory_overshoot_bytes: u64,
    /// Peak decoded attributed-memory bytes attributed to the active body pipeline.
    pub(crate) peak_active_pipeline_decoded_attributed_memory_bytes: u64,
    /// Decoded attributed-memory bytes in the last state sample before harness teardown.
    ///
    /// This can be nonzero because reaching the target ends the scenario before
    /// the terminal task-drop view is emitted to the trace.
    pub(crate) final_active_pipeline_decoded_attributed_memory_bytes: u64,
    /// Every state row's decoded stage totals equal its aggregate pipeline total.
    pub(crate) decoded_stage_totals_match: bool,
    /// Final reserved download bytes (leak detector once quiesced).
    pub(crate) final_budget_reserved: u64,
    /// Liveness-reaper / protocol-reject disconnects observed.
    pub(crate) protocol_rejects: usize,
    /// Total `block_get_blocks_sent` requests issued over the run. Exceeds the chain
    /// length when blocks are re-requested (a peer dropped/withheld a height), so it is
    /// the non-vacuous signal that a timeout/re-request scenario actually re-requested.
    pub(crate) total_requests: usize,
    /// Worst per-peer streak of `GetBlocks` requests without an accepted block body.
    pub(crate) max_requests_without_block_progress: u64,
    /// Worst per-peer no-progress streak observed before a peer has delivered its
    /// first accepted block body.
    pub(crate) max_unproven_requests_without_block_progress: u64,
    /// `block_get_blocks_sent` requests issued via the floor bypass (a floor request
    /// sent while the peer was saturated at its BBR cwnd).
    pub(crate) floor_bypass_requests: usize,
    /// Peak per-peer byte cwnd observed on a `block_body_received` row (`bbr_cwnd_bytes`,
    /// emitted only under the byte unit). `0` means the field never appeared (blocks
    /// unit, or no completed deliveries).
    pub(crate) peak_cwnd_bytes: u64,
    /// Peak per-peer in-flight reserved bytes observed (`bbr_inflight_bytes`).
    pub(crate) peak_inflight_bytes: u64,
    /// Peak per-peer derived byte→request capacity observed (`bbr_cwnd`, the byte cwnd
    /// divided by a representative body). Under the byte unit this scales as
    /// `cwnd_bytes / body_size`, so it is the clean signal that request depth tracks
    /// the inverse of body size.
    pub(crate) peak_cwnd_requests: u64,
    /// Lowest per-peer reliability (goodput per-mille, `0..=1000`) observed on any
    /// `block_body_received` row. `1000` means no peer's request drops ever registered;
    /// a value below `1000` proves the reliability discount engaged end-to-end for a
    /// request-dropping carrier.
    pub(crate) min_reliability_permille: u64,
    /// The last per-peer byte cwnd observed in the run (`bbr_cwnd_bytes`, byte unit only),
    /// across `block_body_received` and `block_peer_bbr` heartbeat rows. Paired with
    /// [`peak_cwnd_bytes`](Self::peak_cwnd_bytes) it shows a peer whose bandwidth dropped
    /// mid-run settling to a *smaller* window (the controller adapting — "kept but
    /// weaker") rather than being cut off. `0` if the field never appeared.
    pub(crate) final_cwnd_bytes: u64,
    /// The last reliability (goodput per-mille) observed in the run. Unlike
    /// [`min_reliability_permille`](Self::min_reliability_permille) (which captures the
    /// deepest transient trough), this is the settled value — a peer that slowed but keeps
    /// delivering recovers here as its late bodies credit back, distinguishing it from a
    /// wedged peer whose reliability stays collapsed.
    pub(crate) final_reliability_permille: u64,
}

/// Extract the report from a flushed trace reader and the post-teardown live counter.
pub(crate) fn report(reader: &TraceReader, final_retained_memory_bytes: u64) -> InvariantReport {
    let state_rows: Vec<&Value> = reader
        .table("block_sync")
        .rows()
        .into_iter()
        .filter(|row| event(row) == Some("block_sync_state"))
        .collect();

    let max_outstanding = state_rows
        .iter()
        .filter_map(|row| u64_field(row, "outstanding"))
        .max()
        .unwrap_or(0);
    let peak_budget_reserved = state_rows
        .iter()
        .filter_map(|row| u64_field(row, "budget_reserved"))
        .max()
        .unwrap_or(0);
    let final_budget_reserved = state_rows
        .iter()
        .rev()
        .find_map(|row| u64_field(row, "budget_reserved"))
        .unwrap_or(0);
    let peak_retained_pipeline_wire_bytes = state_rows
        .iter()
        .filter_map(|row| u64_field(row, "retained_pipeline_wire_bytes"))
        .max()
        .unwrap_or(0);
    let peak_retained_memory_bytes = state_rows
        .iter()
        .filter_map(|row| u64_field(row, "retained_memory_bytes"))
        .max()
        .unwrap_or(0);
    let peak_retained_memory_overshoot_bytes = state_rows
        .iter()
        .filter_map(|row| u64_field(row, "retained_memory_overshoot_bytes"))
        .max()
        .unwrap_or(0);
    let peak_active_pipeline_decoded_attributed_memory_bytes = state_rows
        .iter()
        .filter_map(|row| u64_field(row, "active_pipeline_decoded_attributed_memory_bytes"))
        .max()
        .unwrap_or(0);
    let final_active_pipeline_decoded_attributed_memory_bytes = state_rows
        .iter()
        .rev()
        .find_map(|row| u64_field(row, "active_pipeline_decoded_attributed_memory_bytes"))
        .unwrap_or(0);
    let decoded_stage_totals_match = state_rows.iter().all(|row| {
        let stage_total = u64_field(row, "sequencer_input_decoded_attributed_memory_bytes")
            .unwrap_or(0)
            .saturating_add(u64_field(row, "reorder_decoded_attributed_memory_bytes").unwrap_or(0))
            .saturating_add(
                u64_field(row, "applying_decoded_attributed_memory_bytes").unwrap_or(0),
            );
        u64_field(row, "active_pipeline_decoded_attributed_memory_bytes") == Some(stage_total)
    });
    let protocol_rejects = reader
        .table("block_sync")
        .count("block_peer_protocol_reject");
    let total_requests = reader.table("block_sync").count("block_get_blocks_sent");
    let max_requests_without_block_progress = max_requests_without_block_progress(reader);
    let max_unproven_requests_without_block_progress =
        max_unproven_requests_without_block_progress(reader);
    let body_rows: Vec<&Value> = reader
        .table("block_sync")
        .rows()
        .into_iter()
        .filter(|row| event(row) == Some("block_body_received"))
        .collect();
    let floor_bypass_requests = reader
        .table("block_sync")
        .rows()
        .into_iter()
        .filter(|row| event(row) == Some("block_get_blocks_sent"))
        .filter(|row| u64_field(row, "floor_bypass") == Some(1))
        .count();
    let peak_cwnd_bytes = body_rows
        .iter()
        .filter_map(|row| u64_field(row, "bbr_cwnd_bytes"))
        .max()
        .unwrap_or(0);
    let peak_inflight_bytes = body_rows
        .iter()
        .filter_map(|row| u64_field(row, "bbr_inflight_bytes"))
        .max()
        .unwrap_or(0);
    let peak_cwnd_requests = body_rows
        .iter()
        .filter_map(|row| u64_field(row, "bbr_cwnd"))
        .max()
        .unwrap_or(0);
    // Reliability is emitted on both `block_get_blocks_sent` (request time, where it
    // discounts the cwnd) and `block_body_received` rows, so scan the whole table: a
    // dropping peer keeps requesting at a falling reliability even when it stops
    // delivering.
    let min_reliability_permille = reader
        .table("block_sync")
        .rows()
        .into_iter()
        .filter_map(|row| u64_field(row, "bbr_reliability_permille"))
        .min()
        .unwrap_or(1000);
    // The byte cwnd emitted last in the run — the settled window after any mid-run
    // bandwidth change. `block_peer_bbr` heartbeats keep this fresh even when a peer stops
    // completing deliveries.
    let final_cwnd_bytes = reader
        .table("block_sync")
        .rows()
        .into_iter()
        .rev()
        .find_map(|row| u64_field(row, "bbr_cwnd_bytes"))
        .unwrap_or(0);
    let final_reliability_permille = reader
        .table("block_sync")
        .rows()
        .into_iter()
        .rev()
        .find_map(|row| u64_field(row, "bbr_reliability_permille"))
        .unwrap_or(1000);

    InvariantReport {
        state_samples: state_rows.len(),
        max_outstanding,
        peak_budget_reserved,
        peak_retained_pipeline_wire_bytes,
        peak_retained_memory_bytes,
        final_retained_memory_bytes,
        peak_retained_memory_overshoot_bytes,
        peak_active_pipeline_decoded_attributed_memory_bytes,
        final_active_pipeline_decoded_attributed_memory_bytes,
        decoded_stage_totals_match,
        final_budget_reserved,
        protocol_rejects,
        total_requests,
        max_requests_without_block_progress,
        max_unproven_requests_without_block_progress,
        floor_bypass_requests,
        peak_cwnd_bytes,
        peak_inflight_bytes,
        peak_cwnd_requests,
        min_reliability_permille,
        final_cwnd_bytes,
        final_reliability_permille,
    }
}

/// Assert that every RAII retained-body charge was released during teardown.
pub(crate) fn assert_retained_memory_drained(report: &InvariantReport) {
    assert_eq!(
        report.final_retained_memory_bytes, 0,
        "retained body memory must drain to zero after teardown",
    );
}

/// Assert every accounting ledger drained to exactly zero after teardown.
///
/// Unlike the trace-derived bounds this reads the live ledgers and permits no slack:
/// once every owner is gone there is no handoff left to be mid-flight, so any nonzero
/// value is a charge some drop path failed to release.
pub(crate) fn assert_accounting_drained(outcome: &FuzzOutcome) {
    let accounting = outcome.final_accounting;
    let leaked: Vec<(&'static str, u64)> = [
        ("wire_budget_reserved", accounting.wire_budget_reserved),
        ("work_reserved", accounting.work_reserved),
        ("work_reserved_scanned", accounting.work_reserved_scanned),
        ("retained_total", accounting.retained_total),
        (
            "in_flight_reservation_bytes",
            accounting.in_flight_reservation_bytes,
        ),
        (
            "in_flight_reservation_count",
            accounting.in_flight_reservation_count,
        ),
        ("sequencer_input_bytes", accounting.sequencer_input_bytes),
        (
            "sequencer_input_decoded",
            accounting.sequencer_input_decoded,
        ),
    ]
    .into_iter()
    .filter(|(_, value)| *value != 0)
    .collect();

    assert!(
        leaked.is_empty(),
        "block-sync accounting must drain to zero after teardown; leaked {leaked:?} \
         (full snapshot: {accounting:?})",
    );
}

fn max_unproven_requests_without_block_progress(reader: &TraceReader) -> u64 {
    reader
        .table("block_sync")
        .rows()
        .into_iter()
        .filter(|row| event(row) == Some("block_get_blocks_sent"))
        .filter(|row| u64_field(row, "block_progress_proven") == Some(0))
        .filter_map(|row| u64_field(row, "requests_without_block_progress"))
        .max()
        .unwrap_or(0)
}

fn max_requests_without_block_progress(reader: &TraceReader) -> u64 {
    let mut streaks: HashMap<String, u64> = HashMap::new();
    let mut max_streak = 0u64;

    for row in reader.table("block_sync").rows() {
        let Some(peer) = str_field(row, "peer") else {
            continue;
        };

        match event(row) {
            Some("block_peer_connected") => {
                streaks.insert(peer.to_string(), 0);
            }
            Some("block_get_blocks_sent") => {
                let streak = streaks
                    .entry(peer.to_string())
                    .and_modify(|streak| *streak = streak.saturating_add(1))
                    .or_insert(1);
                max_streak = max_streak.max(*streak);
            }
            Some("block_body_received")
            | Some("block_peer_disconnected")
            | Some("block_peer_protocol_reject") => {
                streaks.insert(peer.to_string(), 0);
            }
            _ => {}
        }
    }

    max_streak
}

/// Assert the run's core invariants. `outstanding_slack` is added to the per-peer
/// advertised-inflight sum to absorb brief over-counts at request boundaries.
pub(crate) fn assert_core(
    scenario: &Scenario,
    outcome: &FuzzOutcome,
    report: &InvariantReport,
    outstanding_slack: u64,
) {
    // No deadlock / stall, and (via the in-order mock committer) a contiguous,
    // hash-correct committed prefix `1..=target`.
    assert!(
        outcome.reached_target(),
        "sync stalled at {} of {} (state_samples={}, max_outstanding={}, rejects={})",
        outcome.committed_tip.0,
        outcome.target.0,
        report.state_samples,
        report.max_outstanding,
        report.protocol_rejects,
    );

    // Tracing actually produced the rows the analysis scripts consume.
    assert!(
        report.state_samples > 0,
        "run emitted no block_sync_state rows",
    );
    assert!(
        report.decoded_stage_totals_match,
        "decoded pipeline aggregate drifted from its stage totals",
    );
    assert!(
        report.peak_active_pipeline_decoded_attributed_memory_bytes > 0,
        "successful run never observed active decoded pipeline bytes",
    );
    assert_retained_memory_drained(report);
    assert_accounting_drained(outcome);

    // Per-peer windows respect the advertised inflight caps: aggregate in-flight must
    // not exceed the sum of per-peer advertised `max_inflight_requests`.
    let outstanding_bound: u64 = scenario
        .peers
        .iter()
        .map(|peer| u64::from(peer.max_inflight_requests))
        .sum::<u64>()
        .saturating_add(outstanding_slack);
    assert!(
        report.max_outstanding <= outstanding_bound,
        "aggregate outstanding {} exceeded the advertised-inflight bound {}",
        report.max_outstanding,
        outstanding_bound,
    );

    // The request budget permits at most one floor-request overdraft.
    let overdraft_slack = scenario.config.floor_request_byte_reservation();
    assert!(
        report.peak_budget_reserved
            <= scenario
                .config
                .max_inflight_block_bytes
                .saturating_add(overdraft_slack),
        "peak reserved bytes {} exceeded the in-flight request budget {} (+{} floor-overdraft slack)",
        report.peak_budget_reserved,
        scenario.config.max_inflight_block_bytes,
        overdraft_slack,
    );

    // The final snapshot can race one receipt handoff.
    assert!(
        report.final_budget_reserved <= overdraft_slack,
        "reserved request bytes {} must drain to zero at quiescence (allowing one \
         in-flight receipt release, <= {})",
        report.final_budget_reserved,
        overdraft_slack,
    );
}

fn event(row: &Value) -> Option<&str> {
    row.get("event").and_then(Value::as_str)
}

fn u64_field(row: &Value, field: &str) -> Option<u64> {
    row.get(field).and_then(Value::as_u64)
}

fn str_field<'a>(row: &'a Value, field: &str) -> Option<&'a str> {
    row.get(field).and_then(Value::as_str)
}
