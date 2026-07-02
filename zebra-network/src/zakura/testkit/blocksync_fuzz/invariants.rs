//! Invariant checks over a finished fuzzer run.
//!
//! These turn a scenario from a sim into a fuzzer: a violation fails the test (and,
//! with `ZAKURA_TEST_TRACE=keep`, persists the trace for the analysis scripts). The
//! strongest correctness signal is "reached target", because the mock commit pipeline
//! (`MockApplyFrontier`) only advances on an in-order, hash-correct body — so reaching
//! the target proves every height committed exactly once, contiguously, with the
//! corpus hash. The trace-derived bounds catch download-side regressions.

use serde_json::Value;

use super::scenario::{FuzzOutcome, Scenario};
use crate::zakura::testkit::TraceReader;

/// Aggregate facts extracted from one run's `block_sync` trace table.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct InvariantReport {
    /// Number of `block_sync_state` snapshots emitted (tracing liveness).
    pub(crate) state_samples: usize,
    /// Peak aggregate in-flight requests across all peers.
    pub(crate) max_outstanding: u64,
    /// Peak reserved download bytes (memory pressure).
    pub(crate) peak_budget_reserved: u64,
    /// Peak retained pipeline wire bytes (`sequencer_input + reorder + applying`), the
    /// wire-byte footprint of bodies actually held in memory. Multiplied by the
    /// deserialized-memory factor this approximates peak resident cost — unlike
    /// `peak_budget_reserved`, it excludes reservations for bytes not yet received.
    pub(crate) peak_retained_pipeline_wire_bytes: u64,
    /// Final reserved download bytes (leak detector once quiesced).
    pub(crate) final_budget_reserved: u64,
    /// Liveness-reaper / protocol-reject disconnects observed.
    pub(crate) protocol_rejects: usize,
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
}

/// Extract the report from a flushed trace reader.
pub(crate) fn report(reader: &TraceReader) -> InvariantReport {
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
    let protocol_rejects = reader
        .table("block_sync")
        .count("block_peer_protocol_reject");
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

    InvariantReport {
        state_samples: state_rows.len(),
        max_outstanding,
        peak_budget_reserved,
        peak_retained_pipeline_wire_bytes,
        final_budget_reserved,
        protocol_rejects,
        floor_bypass_requests,
        peak_cwnd_bytes,
        peak_inflight_bytes,
        peak_cwnd_requests,
    }
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

    // The global byte budget is never over-committed: peak reserved download bytes
    // (in-flight + reorder + applying) must stay within the configured ceiling. Every
    // per-peer routine reserves against the same CAS-guarded `ByteBudget`, so this must
    // hold no matter how many peers race — the memory bound the spec requires. Vacuous
    // only for scenarios that set an effectively unbounded budget (`u64::MAX`); the
    // tight-ceiling scenarios make it bite.
    assert!(
        report.peak_budget_reserved <= scenario.config.max_inflight_block_bytes,
        "peak reserved bytes {} exceeded the global in-flight byte budget {}",
        report.peak_budget_reserved,
        scenario.config.max_inflight_block_bytes,
    );
}

fn event(row: &Value) -> Option<&str> {
    row.get("event").and_then(Value::as_str)
}

fn u64_field(row: &Value, field: &str) -> Option<u64> {
    row.get(field).and_then(Value::as_u64)
}
