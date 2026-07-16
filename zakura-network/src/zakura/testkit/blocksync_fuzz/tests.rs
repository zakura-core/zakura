//! Named block-sync fuzzer scenarios + invariant assertions.
//!
//! Each test drives the real reactor through a distinct adversarial shape and asserts
//! the core invariants (no stall, contiguous/correct commit, bounded in-flight). They
//! emit the standard JSONL; run with `ZAKURA_TEST_TRACE=keep` and point the analysis
//! scripts at `target/zakura-traces/<name>/node-00` to inspect a run.

use std::time::Duration;

use zakura_chain::block;

use super::{
    assert_core_invariants, fuzz_config, invariant_report, run_scenario, run_trace,
    CommitBurstStall, CommitProfile, Degrade, DegradeMode, FuzzOutcome, IdleGap, InvariantReport,
    LatencyDist, PeerSpec, Scenario, ServeProfile, TipEvent, TipEventKind,
};
use crate::zakura::{
    ZakuraBlockSyncConfig, DESERIALIZED_MEM_FACTOR, MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES,
};

/// Run a scenario, flush its trace, assert the core invariants, and return the outcome
/// + report. `outstanding_slack` absorbs brief over-counts at request boundaries.
async fn run_checked(
    name: &str,
    scenario: Scenario,
    outstanding_slack: u64,
) -> (FuzzOutcome, InvariantReport) {
    let (mut capture, trace) = run_trace(name).expect("trace capture opens");
    let outcome = run_scenario(&scenario, trace)
        .await
        .expect("scenario runs without harness error");
    capture.flush().await;
    let reader = capture
        .reader()
        .expect("trace reader loads the flushed run");
    let report = invariant_report(&reader);
    tracing::info!(
        scenario = name,
        committed = outcome.committed_tip.0,
        target = outcome.target.0,
        state_samples = report.state_samples,
        max_outstanding = report.max_outstanding,
        peak_budget_reserved = report.peak_budget_reserved,
        final_budget_reserved = report.final_budget_reserved,
        protocol_rejects = report.protocol_rejects,
        floor_bypass_requests = report.floor_bypass_requests,
        total_requests = report.total_requests,
        max_requests_without_block_progress = report.max_requests_without_block_progress,
        max_unproven_requests_without_block_progress =
            report.max_unproven_requests_without_block_progress,
        min_reliability_permille = report.min_reliability_permille,
        "blocksync fuzz scenario complete",
    );
    assert_core_invariants(&scenario, &outcome, &report, outstanding_slack);
    capture.finish().await.expect("capture discards cleanly");
    (outcome, report)
}

/// A config with a short request timeout, for scenarios that rely on re-requesting
/// around a slow/withholding/dropping peer within the deadline.
fn retry_config() -> ZakuraBlockSyncConfig {
    ZakuraBlockSyncConfig {
        request_timeout: Duration::from_secs(2),
        ..fuzz_config()
    }
}

fn target(blocks: u32) -> block::Height {
    block::Height(blocks)
}

/// Steady state: several fast, full-range peers. Baseline throughput + invariants.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_steady() {
    let blocks = 300;
    let scenario = Scenario::new(
        blocks,
        0x57ea_0001,
        fuzz_config(),
        vec![
            PeerSpec::fast(1, target(blocks)),
            PeerSpec::fast(2, target(blocks)),
            PeerSpec::fast(3, target(blocks)),
        ],
    );
    run_checked("fuzz_steady", scenario, 32).await;
}

/// Steady state under the byte cwnd unit: the controller budgets in-flight
/// work by reserved body bytes instead of request count. End-to-end seam check — the
/// byte-denominated `available_slots` gate must still drive the real reactor to the tip
/// without stalling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_steady_bytes_unit() {
    let blocks = 300;
    let config = ZakuraBlockSyncConfig {
        bbr_cwnd_unit: crate::zakura::CwndUnit::Bytes,
        ..fuzz_config()
    };
    let scenario = Scenario::new(
        blocks,
        0x57ea_0008,
        config,
        vec![
            PeerSpec::fast(1, target(blocks)),
            PeerSpec::fast(2, target(blocks)),
            PeerSpec::fast(3, target(blocks)),
        ],
    );
    run_checked("fuzz_steady_bytes_unit", scenario, 32).await;
}

/// Head-of-line: one genuinely slow, high-RTprop peer alongside two fast byte-accurate
/// carriers, in the single-block-per-request production regime. The floor must ride the
/// carriers (the slow peer's higher RTprop defers it off the floor on the normal take
/// path) while the slow peer is **kept, not reaped** — it serves a body roughly every
/// 0.5 s, well inside the liveness window, so disconnecting it would throw away real
/// bandwidth for no floor benefit. Asserts convergence and zero reaper disconnects; the
/// floor-HoL p99 itself is the live-trace metric.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_one_slow_peer_hol() {
    let blocks = 300;
    let config = ZakuraBlockSyncConfig {
        max_blocks_per_response: 1,
        ..fuzz_config()
    };
    // 64 KiB/s with an 80 ms base RTT ⇒ ~0.5 s per 32 KiB body and a measured RTprop far
    // above the carriers', so the normal-path floor preference defers the floor to them.
    let slow = PeerSpec::with_serve(
        1,
        target(blocks),
        ServeProfile::byte_rate(Duration::from_millis(80), 64 * 1024),
    );
    let carrier = |id| {
        PeerSpec::with_serve(
            id,
            target(blocks),
            ServeProfile::byte_rate(Duration::from_millis(2), 50 * 1024 * 1024),
        )
    };
    let mut scenario = Scenario::new(
        blocks,
        0x57ea_0002,
        config,
        vec![slow, carrier(2), carrier(3)],
    );
    scenario.target_block_bytes = Some(32 * 1024);
    scenario.deadline = Duration::from_secs(60);
    let (_, report) = run_checked("fuzz_one_slow_peer_hol", scenario, 32).await;

    // Directive #1: a peer delivering at a slow-but-steady cadence is never kicked.
    assert_eq!(
        report.protocol_rejects, 0,
        "the slow-but-progressing peer must not be reaped (it serves ~every 0.5 s)",
    );
}

/// Reorg: a mid-sync verified-tip reset, then sync resumes to the target. Stretched
/// with a per-block serve latency so the reset lands while download is in flight.
///
/// Ignored in Phase 1: a faithful *mid-sync* `VerifiedReset` needs the real
/// `Committer`'s epoch / lowest-reset-wins rollback semantics (re-verifying every
/// height above the reset). `MockApplyFrontier` only mirrors part of that, so the
/// re-sync stalls. The header-reanchor "large → small" path through the same
/// `handle_chain_tip_reset` IS covered by `fuzz_large_to_small`. This scenario is the
/// validation target for the high-fidelity `Committer<MockVerifier>` tier.
#[ignore = "needs high-fidelity Committer<MockVerifier> for mid-sync reorg epoch/reset semantics"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_reorg() {
    let blocks = 600;
    let peer = PeerSpec::with_serve(
        1,
        target(blocks),
        ServeProfile::slow(Duration::from_millis(0), Duration::from_millis(2)),
    );
    let mut scenario = Scenario::new(
        blocks,
        0x57ea_0003,
        retry_config(),
        vec![peer, PeerSpec::fast(2, target(blocks))],
    );
    // Reset to a low height the node has already committed past by 300 ms (per-block
    // 2 ms ⇒ ~150 committed), so it is a true rollback, then it re-syncs to the tip.
    scenario.timeline = vec![TipEvent {
        at: Duration::from_millis(300),
        kind: TipEventKind::VerifiedReset(block::Height(50)),
    }];
    scenario.deadline = Duration::from_secs(30);
    let result = tokio::spawn(async move { run_checked("fuzz_reorg", scenario, 32).await }).await;
    assert!(
        result.is_err_and(|error| error.is_panic()),
        "mock reorg scenario should keep failing until the high-fidelity committer tier lands",
    );
}

/// Idle/withholding: one peer is missing a height window (answers `RangeUnavailable`);
/// a covering peer serves it. The node must route around the gap. Deterministic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_idle_peers() {
    let blocks = 300;
    let withholder = PeerSpec::with_serve(
        1,
        target(blocks),
        ServeProfile {
            withhold: Some((block::Height(150), block::Height(180))),
            ..ServeProfile::fast()
        },
    );
    let scenario = Scenario::new(
        blocks,
        0x57ea_0004,
        retry_config(),
        vec![withholder, PeerSpec::fast(2, target(blocks))],
    );
    run_checked("fuzz_idle_peers", scenario, 32).await;
}

/// Silent-dropping carrier: one peer answers normally half the time and **silently
/// drops** the rest (no response at all), alongside one fast full-range peer. This is
/// the bbr-committer-6 floor-stall shape — a peer takes a floor-critical request and
/// never serves it, so the lowest missing height waits on the node's request-timeout /
/// re-request path before a healthy peer covers it. The contiguous-commit invariant
/// (`reached_target`) proves a silently-dropping peer never wedges sync, and the
/// re-request count proves the timeout path was actually exercised (non-vacuous).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_silent_dropping_peer() {
    let blocks = 300;
    let config = ZakuraBlockSyncConfig {
        max_blocks_per_response: 1,
        ..retry_config()
    };
    // A flaky carrier: full-range and otherwise fast, but drops half its requests on the
    // floor — exactly the silent-on-the-floor peer that stalls the contiguous head.
    let flaky = PeerSpec::with_serve(
        1,
        target(blocks),
        ServeProfile {
            drop_probability: 0.5,
            ..ServeProfile::fast()
        },
    );
    let mut scenario = Scenario::new(
        blocks,
        0x57ea_000d,
        config,
        vec![flaky, PeerSpec::fast(2, target(blocks))],
    );
    scenario.deadline = Duration::from_secs(60);
    let (_, report) = run_checked("fuzz_silent_dropping_peer", scenario, 32).await;

    // Non-vacuous: the silent drops forced re-requests, so more requests were issued than
    // there are blocks (otherwise the dropping peer never held a height we needed).
    assert!(
        report.total_requests > usize::try_from(blocks).expect("block count fits usize"),
        "silent drops must force re-requests: issued {} requests for {} blocks",
        report.total_requests,
        blocks,
    );
}

/// Dropping carriers cover the upper half while a fast peer covers only the lower half.
/// Their silent drops time out and age the carriers' goodput EWMAs below 1.0, which the
/// BBR cwnd formula folds into smaller expected windows — the end-to-end proof (through
/// the real routine) that the reliability discount engages, complementing the
/// `bbr::bbr_tests` unit coverage. Sync still completes: the discount never latches cwnd
/// at zero, and redundant upper-half coverage keeps clustered drops from wedging CI
/// coverage runs at the half-chain boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_reliability_discounts_dropping_carrier() {
    let blocks = 120;
    let half = block::Height(blocks / 2);
    let dropping = |id| {
        PeerSpec::with_serve(
            id,
            target(blocks),
            ServeProfile {
                drop_probability: 0.3,
                ..ServeProfile::fast()
            },
        )
    };
    // The fast peer can only serve the lower half, forcing the upper half through the
    // dropping carriers.
    let mut fast = PeerSpec::fast(3, half);
    fast.servable_high = half;

    let config = ZakuraBlockSyncConfig {
        // Keep the short floor-rescue leash from `retry_config`, but give CI coverage
        // builds enough no-progress liveness slack to avoid parking an upper-half carrier
        // during a deterministic cluster of drops.
        request_timeout: Duration::from_secs(4),
        ..retry_config()
    };
    let mut scenario = Scenario::new(
        blocks,
        0x57ea_00c0,
        config,
        vec![dropping(1), dropping(2), fast],
    );
    scenario.deadline = Duration::from_secs(90);
    let (_, report) =
        run_checked("fuzz_reliability_discounts_dropping_carrier", scenario, 32).await;

    assert!(
        report.min_reliability_permille < 1000,
        "a request-dropping carrier must lower its measured reliability (the goodput \
         discount folded into its BBR cwnd), got {}/1000",
        report.min_reliability_permille,
    );
}

/// Fully silent carrier: one peer accepts status and `GetBlocks` but never sends any
/// block-sync response. The node must cap requests to that peer, disconnect it via
/// no-progress liveness, then finish through a healthy peer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_silent_peer_request_cap() {
    let blocks = 96;
    let probe_requests = 1;
    let request_cap = 8;
    let config = ZakuraBlockSyncConfig {
        max_blocks_per_response: 1,
        request_timeout: Duration::from_millis(100),
        floor_rescue_timeout: Duration::from_millis(25),
        initial_block_probe_requests: probe_requests,
        max_requests_without_block_progress: request_cap,
        ..fuzz_config()
    };

    let mut silent = PeerSpec::with_serve(
        1,
        target(blocks),
        ServeProfile {
            drop_probability: 1.0,
            ..ServeProfile::fast()
        },
    );
    silent.max_inflight_requests = 64;

    let mut healthy = PeerSpec::fast(2, target(blocks));
    healthy.connect_at = Duration::from_millis(700);

    let mut scenario = Scenario::new(blocks, 0x57ea_000e, config, vec![silent, healthy]);
    scenario.target_block_bytes = Some(16 * 1024);
    scenario.deadline = Duration::from_secs(10);
    let (_, report) = run_checked("fuzz_silent_peer_request_cap", scenario, 32).await;

    assert!(
        report.protocol_rejects >= 1,
        "the fully silent peer must be disconnected by no-progress liveness",
    );
    assert_eq!(
        report.max_unproven_requests_without_block_progress,
        u64::from(probe_requests),
        "the silent peer should receive exactly the configured initial probe budget",
    );
}

/// Config for the wedge/slow degradation tests: single-block responses and a short
/// request timeout, so the liveness window (`request_timeout × BLOCK_PROGRESS_TIMEOUT_
/// REQUESTS`) elapses well inside the run once a peer stops delivering.
fn degrade_config() -> ZakuraBlockSyncConfig {
    ZakuraBlockSyncConfig {
        max_blocks_per_response: 1,
        request_timeout: Duration::from_millis(100),
        floor_rescue_timeout: Duration::from_millis(25),
        initial_block_probe_requests: 1,
        max_requests_without_block_progress: 8,
        ..fuzz_config()
    }
}

/// Requirement — a peer that WEDGES after making progress is disconnected. The carrier
/// serves normally at first (proving progress, so its no-progress cap opens to the
/// larger proven budget), then goes silent mid-run. The failure mechanism must still
/// seal it (reliability ramps toward zero → zero cwnd, no new work) and the liveness
/// timer must then disconnect it — its early progress must not buy it immunity. A second
/// peer, connecting after the wedge, finishes the sync, proving the wedge did not stall
/// the chain.
///
/// This is the counterpart to `fuzz_silent_peer_request_cap` (which wedges from the
/// start, never proving progress): here the peer is *proven* when it wedges, the harder
/// case the ramp-to-zero seal exists for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_peer_wedges_after_progress_is_disconnected() {
    let blocks = 400;
    // A carrier that serves at a finite rate (so it only gets partway through the chain),
    // then wedges (drops everything) 250 ms in — long enough to deliver a run of bodies
    // first, so it is a *proven* peer when it goes silent, with plenty of chain left.
    let waverer = PeerSpec::with_serve(
        1,
        target(blocks),
        ServeProfile {
            bandwidth_bytes_per_sec: Some(4 * 1024 * 1024),
            degrade: Some(Degrade {
                at: Duration::from_millis(250),
                mode: DegradeMode::GoSilent,
            }),
            ..ServeProfile::fast()
        },
    );
    // A healthy peer that joins only after the wedge has been detected and the waverer
    // disconnected (liveness = request_timeout × 4 = 400 ms, so ~650 ms after the ~250 ms
    // last body), then finishes the remaining heights.
    let mut healthy = PeerSpec::fast(2, target(blocks));
    healthy.connect_at = Duration::from_millis(1_200);

    let mut scenario = Scenario::new(
        blocks,
        0x57ea_00f0,
        degrade_config(),
        vec![waverer, healthy],
    );
    scenario.target_block_bytes = Some(16 * 1024);
    scenario.deadline = Duration::from_secs(20);
    let (_, report) = run_checked(
        "fuzz_peer_wedges_after_progress_is_disconnected",
        scenario,
        32,
    )
    .await;

    // The wedged (but previously-progressing) peer must be disconnected by liveness.
    assert!(
        report.protocol_rejects >= 1,
        "a peer that wedges after making progress must be disconnected, got {} rejects",
        report.protocol_rejects,
    );
    // It was *proven* when it wedged: its no-progress streak was allowed past the single
    // initial probe (the proven cap), distinguishing this from the never-proved case.
    assert!(
        report.max_requests_without_block_progress >= 2,
        "the disconnected peer should have been proven (streak past the initial probe), got {}",
        report.max_requests_without_block_progress,
    );
    // The reliability seal engaged (the discount folded the drops in on the way down).
    assert!(
        report.min_reliability_permille < 1000,
        "the wedged peer's reliability must fall as its requests stop delivering, got {}/1000",
        report.min_reliability_permille,
    );
}

/// Requirement — a peer that WEDGES by *no longer reading our stream* (not merely going
/// silent) must still be disconnected at the liveness deadline. When a peer stops draining
/// our bounded outbound queue, `outbound_capacity()` falls to zero and stays there. The old
/// liveness escape (`Disconnect if outbound_capacity() == 0 → extend`) treated that as our
/// own write congestion and extended the deadline *every* time, indefinitely — so a wedged
/// peer survived until the ~180 s transport idle timeout while we kept queuing requests it
/// never read. The bounded grace fixes this: once our outbound has been continuously full
/// for `request_timeout`, the peer is disconnected at the liveness deadline regardless.
///
/// This is the distinct counterpart to `fuzz_peer_wedges_after_progress_is_disconnected`
/// (which uses `GoSilent`: the peer keeps *reading* and so never fills our outbound, taking
/// the normal disconnect arm). Here the peer stops reading, so the run exercises the escape
/// arm specifically. A small transport queue depth makes the outbound fill quickly (the
/// default 1024 is too large for the node to ever fill given the no-progress cap — which is
/// exactly why this bug was invisible to the earlier tests).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_peer_that_stops_reading_is_disconnected() {
    let blocks = 400;
    // A proven carrier that serves at a finite rate, then stops reading our stream entirely
    // 250 ms in — a truly stuck connection. By then it has delivered a run of bodies, so it
    // is *proven* (its no-progress cap is the larger proven budget), the harder case. It is
    // the only peer: the chain cannot complete once it wedges, and it is not meant to — the
    // property under test is the disconnect. `fuzz_peer_wedges_after_progress_is_disconnected`
    // (a `GoSilent` peer that keeps reading) already covers a healthy peer finishing the
    // chain after a wedge.
    let wedger = PeerSpec::with_serve(
        1,
        target(blocks),
        ServeProfile {
            bandwidth_bytes_per_sec: Some(4 * 1024 * 1024),
            degrade: Some(Degrade {
                at: Duration::from_millis(250),
                mode: DegradeMode::Wedge,
            }),
            ..ServeProfile::fast()
        },
    );

    let mut scenario = Scenario::new(blocks, 0x57ea_dead, degrade_config(), vec![wedger]);
    scenario.target_block_bytes = Some(16 * 1024);
    // A small per-peer transport queue so the node's outbound to the non-reading peer fills
    // (and stays full) quickly — the condition that drives `outbound_capacity()` to zero and
    // exercises the (now-bounded) liveness escape. The default 1024 is far too large for the
    // node to ever fill given the no-progress cap, which is exactly why this bug was
    // invisible to the earlier tests.
    scenario.transport_queue_depth = Some(4);
    scenario.deadline = Duration::from_secs(5);

    // Run WITHOUT the reach-the-target assertion (a lone wedged peer cannot finish the chain);
    // assert the disconnect directly from the report.
    let (mut capture, trace) =
        run_trace("fuzz_peer_that_stops_reading_is_disconnected").expect("trace capture opens");
    let _outcome = run_scenario(&scenario, trace)
        .await
        .expect("scenario runs without harness error");
    capture.flush().await;
    let reader = capture
        .reader()
        .expect("trace reader loads the flushed run");
    let report = invariant_report(&reader);
    capture.finish().await.expect("capture discards cleanly");

    // The wedged, non-reading peer must be disconnected — even though our outbound to it is
    // full (its stream unread). With the old unbounded escape this is 0 (extend forever until
    // the ~180 s transport idle timeout): the teeth of the fix.
    assert!(
        report.protocol_rejects >= 1,
        "a peer that stops reading our stream must still be disconnected at the liveness \
         deadline, got {} rejects",
        report.protocol_rejects,
    );
    // It was proven when it wedged (streak past the single initial probe), so this is the
    // harder proven-peer case, not the never-proved one.
    assert!(
        report.max_requests_without_block_progress >= 2,
        "the disconnected peer should have been proven, got {}",
        report.max_requests_without_block_progress,
    );
}

/// Requirement — a peer that becomes RADICALLY SLOWER (but keeps delivering) is kept,
/// not kicked; its params just adapt. A single full-range carrier serves fast, then
/// drops to a low finite bandwidth behind a high base RTT partway through. Because it is
/// the *only* peer, the run reaches the target **iff** the node keeps it: a wrongful
/// disconnect (mistaking slow-but-delivering for wedged) would stall the sync. The
/// windowed-estimator freshness fix is what keeps its now-slow deliveries inside the
/// (bandwidth-aware) request deadline instead of timing out on a stale-fast estimate and
/// collapsing its reliability.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_peer_slows_radically_is_kept() {
    let blocks = 120;
    // Fast at first with a real RTT (so its BDP-derived byte window rises well above the
    // min-cwnd floor), then radically slower: a 60 ms base RTT and a 512 KiB/s serve —
    // still delivering, just far weaker. A modest inflight cap keeps the pipeline shallow
    // so the fast→slow transition drains without a runaway backlog (the controller's job
    // is to shrink the window, which it does; this keeps the test robust, not flaky).
    let mut slowing = PeerSpec::with_serve(
        1,
        target(blocks),
        ServeProfile {
            first_block_latency: LatencyDist::Fixed(Duration::from_millis(30)),
            bandwidth_bytes_per_sec: Some(64 * 1024 * 1024),
            degrade: Some(Degrade {
                at: Duration::from_millis(300),
                mode: DegradeMode::SlowTo {
                    base_rtt: Duration::from_millis(60),
                    bandwidth_bytes_per_sec: 512 * 1024,
                },
            }),
            ..ServeProfile::fast()
        },
    );
    slowing.max_inflight_requests = 24;

    // A generous request timeout so the deadline is set by the (bandwidth-aware) transfer
    // term, not a tight base — the slow-but-honest deliveries must run to completion. Byte
    // cwnd unit so the window's shrink is observable in `bbr_cwnd_bytes`.
    let config = ZakuraBlockSyncConfig {
        bbr_cwnd_unit: crate::zakura::CwndUnit::Bytes,
        max_blocks_per_response: 1,
        request_timeout: Duration::from_secs(2),
        ..fuzz_config()
    };
    let mut scenario = Scenario::new(blocks, 0x57ea_00f1, config, vec![slowing]);
    scenario.target_block_bytes = Some(16 * 1024);
    scenario.deadline = Duration::from_secs(60);
    let (_, report) = run_checked("fuzz_peer_slows_radically_is_kept", scenario, 32).await;

    // The lone slow-but-delivering peer must be kept: reaching the target (asserted in
    // `run_checked`) already proves it, and there must be zero disconnects.
    assert_eq!(
        report.protocol_rejects, 0,
        "a peer that only slowed down (still delivering) must not be disconnected",
    );
    // It kept delivering, so it was never sealed off like a dropper: its reliability
    // recovers to a healthy settled band (late bodies credit back transition timeouts),
    // well clear of the sealed (~0) range even though a lone slow peer serving its own
    // contiguous floor carries some steady re-request churn.
    assert!(
        report.final_reliability_permille >= 300,
        "a slow-but-delivering peer's reliability must stay well clear of the sealed range \
         (settled {}/1000, trough {}/1000)",
        report.final_reliability_permille,
        report.min_reliability_permille,
    );
    assert!(
        report.final_reliability_permille > report.min_reliability_permille,
        "reliability must recover from its transition trough (settled {} vs trough {})",
        report.final_reliability_permille,
        report.min_reliability_permille,
    );
    // "Params adjust, kept but weaker": its byte window shrank from the fast-phase peak to
    // a smaller settled value, but stayed positive (it keeps a — weaker — window, not
    // sealed to zero and cut off).
    assert!(
        report.final_cwnd_bytes > 0,
        "the slowed peer must keep a (weaker) window, not be sealed to zero",
    );
    assert!(
        report.peak_cwnd_bytes > report.final_cwnd_bytes,
        "the slowed peer's window must adapt downward (peak {} → settled {})",
        report.peak_cwnd_bytes,
        report.final_cwnd_bytes,
    );
}

/// Finding #4 — a peer that answers `RangeUnavailable` for heights it advertised is
/// charged a reliability failure for the heights it left undelivered, not just retried.
/// A withholder advertises the full range but is missing a mid-chain window that no other
/// peer covers until a covering peer joins later; while the floor sits in that window the
/// withholder is asked and repeatedly answers `RangeUnavailable`, so its reliability must
/// fall below 1000. Without the short-response charge those answers would be free.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_range_unavailable_penalizes_reliability() {
    let blocks = 300;
    // Long timeouts so *no request ever times out* in this fast-serving run: that isolates
    // the reliability signal to the short-response (RangeUnavailable) charge alone — a
    // floor-rescue timeout would otherwise also age reliability and mask it.
    let config = ZakuraBlockSyncConfig {
        max_blocks_per_response: 1,
        request_timeout: Duration::from_secs(25),
        floor_rescue_timeout: Duration::from_secs(25),
        ..fuzz_config()
    };
    // Advertises the full range but is missing (100, 200): it answers RangeUnavailable
    // there. A low base RTT makes it the *preferred floor server*, so the floor is offered
    // to it first — guaranteeing it is asked across the withheld window and answers
    // RangeUnavailable there before the covering peer takes over.
    let withholder = PeerSpec::with_serve(
        1,
        target(blocks),
        ServeProfile {
            withhold: Some((block::Height(100), block::Height(200))),
            ..ServeProfile::byte_rate(Duration::from_millis(2), 50 * 1024 * 1024)
        },
    );
    // A full-range covering peer with a higher RTprop (so it backs up the withheld window
    // rather than pre-empting the floor). It serves everything, so the withheld window is
    // always covered and the run reaches the target.
    let coverer = PeerSpec::with_serve(
        2,
        target(blocks),
        ServeProfile::byte_rate(Duration::from_millis(40), 50 * 1024 * 1024),
    );

    let mut scenario = Scenario::new(blocks, 0x57ea_00f2, config, vec![withholder, coverer]);
    scenario.target_block_bytes = Some(16 * 1024);
    scenario.deadline = Duration::from_secs(30);
    let (_, report) =
        run_checked("fuzz_range_unavailable_penalizes_reliability", scenario, 32).await;

    assert!(
        report.min_reliability_permille < 1000,
        "a peer that answers RangeUnavailable for advertised heights must be charged a \
         reliability failure, got {}/1000",
        report.min_reliability_permille,
    );
}

/// Churn storm: a stable peer plus several peers connecting and disconnecting on a
/// staggered schedule. Progress must continue across the churn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_churn_storm() {
    let blocks = 300;
    let stable = PeerSpec::fast(1, target(blocks));
    let mut churn_a = PeerSpec::fast(2, target(blocks));
    churn_a.disconnect_at = Some(Duration::from_millis(100));
    let mut churn_b = PeerSpec::fast(3, target(blocks));
    churn_b.connect_at = Duration::from_millis(50);
    churn_b.disconnect_at = Some(Duration::from_millis(150));
    let mut churn_c = PeerSpec::fast(4, target(blocks));
    churn_c.connect_at = Duration::from_millis(100);
    churn_c.disconnect_at = Some(Duration::from_millis(200));

    let mut scenario = Scenario::new(
        blocks,
        0x57ea_0005,
        fuzz_config(),
        vec![stable, churn_a, churn_b, churn_c],
    );
    scenario.deadline = Duration::from_secs(60);
    run_checked("fuzz_churn_storm", scenario, 32).await;
}

/// Large → small: the header target grows in steps, reanchors down below the current
/// verified tip, then grows again to the full chain. Exercises header advance/reanchor
/// handling and uniform serve jitter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_large_to_small() {
    let blocks = 400;
    let jittery = PeerSpec::with_serve(
        1,
        target(blocks),
        ServeProfile {
            per_block_latency: LatencyDist::Uniform {
                low: Duration::ZERO,
                high: Duration::from_millis(1),
            },
            idle_gap: Some(IdleGap {
                every_responses: 25,
                duration: Duration::from_millis(5),
            }),
            ..ServeProfile::fast()
        },
    );
    let mut scenario = Scenario::new(
        blocks,
        0x57ea_0006,
        fuzz_config(),
        vec![jittery, PeerSpec::fast(2, target(blocks))],
    );
    scenario.initial_best_header = block::Height(100);
    // Space frontier events so a slow CI host can commit past each step before the
    // next reanchor; the production fix still force-refills after reset, but the
    // timeline should not pile Grow/Reanchor events into a few hundred milliseconds.
    scenario.timeline = vec![
        TipEvent {
            at: Duration::from_millis(200),
            kind: TipEventKind::GrowTo(block::Height(200)),
        },
        TipEvent {
            at: Duration::from_millis(500),
            kind: TipEventKind::GrowTo(block::Height(350)),
        },
        TipEvent {
            at: Duration::from_millis(800),
            kind: TipEventKind::HeaderReanchor(block::Height(250)),
        },
        TipEvent {
            at: Duration::from_millis(1_100),
            kind: TipEventKind::GrowTo(block::Height(400)),
        },
    ];
    scenario.deadline = Duration::from_secs(90);
    run_checked("fuzz_large_to_small", scenario, 32).await;
}

/// A byte-cwnd config whose window binds on reserved body bytes rather than the
/// request-count cap: a `min_cwnd_bytes` floor chosen below `max_inflight × body`, with
/// the generous `fuzz_config` byte budget.
///
/// Forces **one block per request** (`max_blocks_per_response = 1`), the regime the byte
/// controller is designed for and the production default — with multi-block ranges a
/// single request can reserve many bodies at once, so the per-request byte cwnd would no
/// longer bound the in-flight bytes (the request *count* is what the cwnd gates).
fn byte_window_config(min_cwnd_bytes: u64) -> ZakuraBlockSyncConfig {
    ZakuraBlockSyncConfig {
        bbr_cwnd_unit: crate::zakura::CwndUnit::Bytes,
        bbr_min_cwnd_bytes: min_cwnd_bytes,
        max_blocks_per_response: 1,
        ..fuzz_config()
    }
}

/// Run one byte-unit scenario with a fixed body size and two byte-accurate peers,
/// returning its invariant report.
async fn run_byte_size_run(
    name: &str,
    seed: u64,
    blocks: u32,
    body_bytes: usize,
    config: ZakuraBlockSyncConfig,
    serve: ServeProfile,
) -> InvariantReport {
    let mut scenario = Scenario::new(
        blocks,
        seed,
        config,
        vec![
            PeerSpec::with_serve(1, target(blocks), serve),
            PeerSpec::with_serve(2, target(blocks), serve),
        ],
    );
    scenario.target_block_bytes = Some(body_bytes);
    scenario.deadline = Duration::from_secs(30);
    let (_, report) = run_checked(name, scenario, 64).await;
    report
}

/// Mixed block sizes: the byte cwnd holds ~the same reserved bytes in flight regardless
/// of body size, so the in-flight *request* depth must scale inversely with the body
/// size — small bodies pack many requests into the byte window, large bodies few. Two
/// runs that differ only in body size make the headline byte-cwnd property a direct,
/// deterministic comparison (the blocks unit, by contrast, would hold the same request
/// count in both).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_mixed_block_sizes() {
    let blocks = 400;
    let config = byte_window_config(512 * 1024);
    // Byte-accurate serve: each block takes `bytes / 20 MiB/s`, so a big block genuinely
    // takes longer and the controller sees a real bytes/sec BtlBw.
    let serve = ServeProfile::byte_rate(Duration::from_millis(2), 20 * 1024 * 1024);

    let small = run_byte_size_run(
        "fuzz_mixed_block_sizes_small",
        0x57ea_0009,
        blocks,
        8 * 1024,
        config.clone(),
        serve,
    )
    .await;
    let large = run_byte_size_run(
        "fuzz_mixed_block_sizes_large",
        0x57ea_000a,
        blocks,
        64 * 1024,
        config,
        serve,
    )
    .await;

    // Byte-cwnd growth can differ between runs under slower instrumentation, so compare
    // request density per cwnd byte rather than absolute request counts. The small-body
    // run should admit many more single-block requests per byte of cwnd than the
    // large-body run: request depth ∝ 1 / body_size, the headline byte-cwnd property the
    // blocks unit cannot express. (Here ~8× the body size ⇒ a clear density gap.)
    let small_request_density =
        u128::from(small.peak_cwnd_requests).saturating_mul(u128::from(large.peak_cwnd_bytes));
    let large_request_density =
        u128::from(large.peak_cwnd_requests).saturating_mul(u128::from(small.peak_cwnd_bytes));
    assert!(
        small_request_density > large_request_density.saturating_mul(4),
        "small bodies should admit many more single-block requests per byte of cwnd \
         (small={} reqs @ {} B cwnd, large={} reqs @ {} B cwnd)",
        small.peak_cwnd_requests,
        small.peak_cwnd_bytes,
        large.peak_cwnd_requests,
        large.peak_cwnd_bytes,
    );
    // The byte window bounds in-flight memory: with one block per request the peak
    // reserved bytes track the byte cwnd (plus a small floor-bypass margin), never
    // ballooning to many multiples of it — the head-of-line bound byte denomination
    // exists to provide. (A multi-block-range regression would push this well past 2×.)
    for (label, run) in [("small", &small), ("large", &large)] {
        let bound = run.peak_cwnd_bytes.saturating_mul(2);
        assert!(
            run.peak_inflight_bytes <= bound,
            "{label}: in-flight reserved bytes {} must stay bounded by ~the byte cwnd \
             (cwnd={} B, bound={} B)",
            run.peak_inflight_bytes,
            run.peak_cwnd_bytes,
            bound,
        );
    }
}

/// A bursty commit stall builds resident pressure without wedging progress.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_commit_stall() {
    let blocks = 400;
    let body_bytes = 32 * 1024usize;
    // A sub-range resident budget forces retention backpressure in the test harness.
    let byte_ceiling: u64 = 2 * 1024 * 1024;
    let resident_budget = byte_ceiling * DESERIALIZED_MEM_FACTOR;
    let config = ZakuraBlockSyncConfig {
        max_reorder_lookahead_bytes: resident_budget,
        max_blocks_per_response: 1,
        ..fuzz_config()
    };
    let mut scenario = Scenario::new(
        blocks,
        0x57ea_000c,
        config,
        vec![
            PeerSpec::fast(1, target(blocks)),
            PeerSpec::fast(2, target(blocks)),
        ],
    );
    scenario.target_block_bytes = Some(body_bytes);
    // A sawtoothing drain holds retention at the gate without wedging.
    scenario.commit = CommitProfile {
        per_commit_delay: Duration::from_millis(1),
        burst: Some(CommitBurstStall {
            every_commits: 40,
            duration: Duration::from_millis(120),
        }),
    };
    scenario.deadline = Duration::from_secs(60);
    let (_, report) = run_checked("fuzz_commit_stall", scenario, 64).await;

    // Budget plus the commit-window exemption and request-boundary margin.
    let window_slack = (MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES as u64)
        .saturating_mul(body_bytes as u64)
        .saturating_mul(DESERIALIZED_MEM_FACTOR);
    let margin = (body_bytes as u64)
        .saturating_mul(16)
        .saturating_mul(DESERIALIZED_MEM_FACTOR);
    let peak_retained_resident = report
        .peak_retained_pipeline_wire_bytes
        .saturating_mul(DESERIALIZED_MEM_FACTOR);
    let bound = resident_budget
        .saturating_add(window_slack)
        .saturating_add(margin);
    assert!(
        peak_retained_resident <= bound,
        "peak retained resident cost {} must stay within the {} B budget \
         (+{} B commit-window slack, +{} B margin); the apply backlog is bounded by \
         the resident gate, not unbounded by the commit stall",
        peak_retained_resident,
        resident_budget,
        window_slack,
        margin,
    );
    // Ensure the bound is exercised.
    assert!(
        peak_retained_resident >= resident_budget / 2,
        "the commit stall should have created real retained-memory pressure \
         (retained {} of the {} B budget)",
        peak_retained_resident,
        resident_budget,
    );
    tracing::info!(
        peak_retained_pipeline_wire_bytes = report.peak_retained_pipeline_wire_bytes,
        peak_retained_resident,
        final_budget_reserved = report.final_budget_reserved,
        resident_budget,
        "commit_stall retention backpressure observation",
    );
}

/// The retained-resident plateau under a commit stall. The
/// resident look-ahead gate must hold the retained pipeline (sequencer input + reorder +
/// applying, at the decoded multiple) near the configured budget plus at most one
/// commit-window worth of exempt bodies — never growing toward the whole chain the way
/// the pre-gate escalator did. The chain is longer than the exempt window so the gate
/// genuinely binds, and the in-flight wire budget is left roomy so the *resident* gate,
/// not the wire budget, is what bounds retention.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_commit_stall_resident_plateau() {
    let blocks = 1_200;
    let body_bytes = 32 * 1024usize;
    // Resident budget: 8 MiB = 2 MiB of retained wire at the ×4 multiple (~64 bodies of
    // gated retention), far below the ~37.5 MB chain. (The fuzzer reactor path does not
    // call the config clamps, so a sub-checkpoint-range budget is usable here; the mock
    // committer needs no checkpoint batch.)
    let resident_budget: u64 = 8 * 1024 * 1024;
    let config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: 64 * 1024 * 1024,
        max_reorder_lookahead_bytes: resident_budget,
        max_blocks_per_response: 1,
        ..fuzz_config()
    };
    let mut scenario = Scenario::new(
        blocks,
        0x57ea_000d,
        config,
        vec![
            PeerSpec::fast(1, target(blocks)),
            PeerSpec::fast(2, target(blocks)),
        ],
    );
    scenario.target_block_bytes = Some(body_bytes);
    scenario.commit = CommitProfile {
        per_commit_delay: Duration::from_millis(1),
        burst: Some(CommitBurstStall {
            every_commits: 40,
            duration: Duration::from_millis(120),
        }),
    };
    scenario.deadline = Duration::from_secs(120);
    let (_, report) = run_checked("fuzz_commit_stall_resident_plateau", scenario, 64).await;

    // Peak retained resident cost stays within the budget, plus the commit-window
    // exemption (one checkpoint range of bodies above the verified tip bypasses the
    // gate) and a small request-boundary margin. A gate regression (the
    // escalator, or reservations invisible to the byte gate) drives retention toward
    // the full ~150 MB resident chain instead.
    // `usize → u64` widenings are lossless on all supported (64-bit) targets.
    let window_slack = (MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES as u64)
        .saturating_mul(body_bytes as u64)
        .saturating_mul(DESERIALIZED_MEM_FACTOR);
    let margin = (body_bytes as u64)
        .saturating_mul(16)
        .saturating_mul(DESERIALIZED_MEM_FACTOR);
    let peak_retained_resident = report
        .peak_retained_pipeline_wire_bytes
        .saturating_mul(DESERIALIZED_MEM_FACTOR);
    let bound = resident_budget
        .saturating_add(window_slack)
        .saturating_add(margin);
    assert!(
        peak_retained_resident <= bound,
        "peak retained resident cost {} must stay within the {} B budget \
         (+{} B commit-window slack, +{} B margin)",
        peak_retained_resident,
        resident_budget,
        window_slack,
        margin,
    );
    // Non-vacuous: the stall actually pushed retention past the gated budget alone, so
    // the bound above is doing real work.
    assert!(
        peak_retained_resident >= resident_budget / 2,
        "the commit stall should have created real retained-memory pressure \
         (retained {} of the {} B budget)",
        peak_retained_resident,
        resident_budget,
    );
    tracing::info!(
        peak_retained_pipeline_wire_bytes = report.peak_retained_pipeline_wire_bytes,
        peak_retained_resident,
        resident_budget,
        window_slack,
        "commit_stall resident-plateau observation",
    );
}

/// The resident plateau with **multi-block responses** (the fuzz-default 16 blocks per
/// request, unlike the single-block pin above). Multi-block takes are the regime where a
/// take whose admission-checked start is inside the commit window could carry
/// above-window heights past a full resident gate if the take geometry were sized by the
/// in-flight budget instead of clamped at the window top (`admit`'s never-span-the-
/// boundary rule). This is end-to-end coverage of the multi-block regime; the bound's
/// commit-window slack is larger than a per-crossing overshoot at this block size, so
/// the *pin* for the take geometry itself is the unit test
/// `exempt_take_never_spans_the_commit_window_boundary` and the `admit` proptest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_commit_stall_resident_plateau_multiblock() {
    let blocks = 1_200;
    let body_bytes = 32 * 1024usize;
    let resident_budget: u64 = 8 * 1024 * 1024;
    let config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: 64 * 1024 * 1024,
        max_reorder_lookahead_bytes: resident_budget,
        // Deliberately NOT pinned to 1: fuzz_config()'s 16-block responses exercise
        // window-crossing take geometry.
        ..fuzz_config()
    };
    let mut scenario = Scenario::new(
        blocks,
        0x57ea_000e,
        config,
        vec![
            PeerSpec::fast(1, target(blocks)),
            PeerSpec::fast(2, target(blocks)),
        ],
    );
    scenario.target_block_bytes = Some(body_bytes);
    scenario.commit = CommitProfile {
        per_commit_delay: Duration::from_millis(1),
        burst: Some(CommitBurstStall {
            every_commits: 40,
            duration: Duration::from_millis(120),
        }),
    };
    scenario.deadline = Duration::from_secs(120);
    let (_, report) = run_checked(
        "fuzz_commit_stall_resident_plateau_multiblock",
        scenario,
        64,
    )
    .await;

    // Same bound as the single-block plateau: budget + one commit window of exempt
    // bodies + a small request-boundary margin. A take-geometry regression (an exempt
    // multi-block take extending above the window sized by the in-flight budget) drives
    // retention toward the full ~150 MB resident chain instead.
    // `usize → u64` widenings are lossless on all supported (64-bit) targets.
    let window_slack = (MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES as u64)
        .saturating_mul(body_bytes as u64)
        .saturating_mul(DESERIALIZED_MEM_FACTOR);
    let margin = (body_bytes as u64)
        .saturating_mul(16)
        .saturating_mul(DESERIALIZED_MEM_FACTOR);
    let peak_retained_resident = report
        .peak_retained_pipeline_wire_bytes
        .saturating_mul(DESERIALIZED_MEM_FACTOR);
    let bound = resident_budget
        .saturating_add(window_slack)
        .saturating_add(margin);
    assert!(
        peak_retained_resident <= bound,
        "peak retained resident cost {} must stay within the {} B budget \
         (+{} B commit-window slack, +{} B margin) with multi-block responses",
        peak_retained_resident,
        resident_budget,
        window_slack,
        margin,
    );
    assert!(
        peak_retained_resident >= resident_budget / 2,
        "the commit stall should have created real retained-memory pressure \
         (retained {} of the {} B budget)",
        peak_retained_resident,
        resident_budget,
    );
    tracing::info!(
        peak_retained_pipeline_wire_bytes = report.peak_retained_pipeline_wire_bytes,
        peak_retained_resident,
        resident_budget,
        window_slack,
        "commit_stall multi-block resident-plateau observation",
    );
}

/// A single high-bandwidth peer with real headroom, served byte-accurately, under the
/// byte unit. The controller must drive a clean sync to the tip while keeping the byte
/// window the binding constraint — a per-peer byte cwnd is traced and the in-flight
/// reserved bytes track it (the controller reasons in bytes, not request slots).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_high_bw_fast_peer() {
    let blocks = 500;
    let config = byte_window_config(256 * 1024);
    let peer = PeerSpec::with_serve(
        1,
        target(blocks),
        // 100 MiB/s link with a 20 ms base RTT: enough headroom that the byte-BDP can
        // exceed the floor once concurrency builds.
        ServeProfile::byte_rate(Duration::from_millis(20), 100 * 1024 * 1024),
    );
    let mut scenario = Scenario::new(blocks, 0x57ea_000b, config, vec![peer]);
    scenario.target_block_bytes = Some(16 * 1024);
    scenario.deadline = Duration::from_secs(30);
    let (_, report) = run_checked("fuzz_high_bw_fast_peer", scenario, 64).await;

    // A per-peer byte cwnd was traced (byte denomination is live) and never dipped below
    // its floor; the in-flight reserved bytes were tracked too.
    assert!(
        report.peak_cwnd_bytes >= 256 * 1024,
        "byte cwnd should be traced at or above the floor, got {} B",
        report.peak_cwnd_bytes,
    );
    assert!(
        report.peak_inflight_bytes > 0,
        "byte in-flight occupancy should be traced",
    );
    tracing::info!(
        peak_cwnd_bytes = report.peak_cwnd_bytes,
        peak_inflight_bytes = report.peak_inflight_bytes,
        max_outstanding = report.max_outstanding,
        "high_bw_fast_peer byte-window observation",
    );
}

/// Lossy peer: a peer silently drops ~30% of requests (no response at all), forcing the
/// node's request-timeout / re-request path, while a covering fast peer can serve every
/// height. The node must route around the drops and still commit a contiguous, correct
/// prefix to the target. Drives the `drop_probability` serve knob no other scenario sets.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_lossy_peer() {
    let blocks = 300;
    let lossy = PeerSpec::with_serve(
        1,
        target(blocks),
        ServeProfile {
            drop_probability: 0.3,
            ..ServeProfile::fast()
        },
    );
    let mut scenario = Scenario::new(
        blocks,
        0x57ea_000d,
        // Short request timeout so dropped requests are re-requested well within the run.
        retry_config(),
        vec![lossy, PeerSpec::fast(2, target(blocks))],
    );
    scenario.deadline = Duration::from_secs(60);
    run_checked("fuzz_lossy_peer", scenario, 32).await;
}

/// Reverse-order serving: peers return the blocks of each multi-block response high→low,
/// exercising out-of-order body arrival and the reorder buffer. `fuzz_config`'s
/// `max_blocks_per_response = 16` lets the node issue multi-block ranges, so the reversal
/// is non-trivial. The node must still commit a contiguous, hash-correct prefix to the
/// target. Drives the `reorder` serve knob no other scenario sets.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_reorder() {
    let blocks = 300;
    let reorder_serve = ServeProfile {
        reorder: true,
        ..ServeProfile::fast()
    };
    let scenario = Scenario::new(
        blocks,
        0x57ea_000e,
        fuzz_config(),
        vec![
            PeerSpec::with_serve(1, target(blocks), reorder_serve),
            PeerSpec::with_serve(2, target(blocks), reorder_serve),
        ],
    );
    run_checked("fuzz_reorder", scenario, 32).await;
}

/// Many peers race against a tight shared request budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_multi_peer_tight_budget() {
    let blocks = 400;
    let body_bytes = 32 * 1024usize;
    // Opening demand exceeds the ceiling, forcing reservation contention.
    let byte_ceiling: u64 = 3 * 1024 * 1024;
    let config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: byte_ceiling,
        max_blocks_per_response: 1,
        ..fuzz_config()
    };
    let peers: Vec<_> = (1u8..=8)
        .map(|id| PeerSpec::fast(id, target(blocks)))
        .collect();
    let mut scenario = Scenario::new(blocks, 0x57ea_000f, config, peers);
    scenario.target_block_bytes = Some(body_bytes);
    // Slow commit creates repeated fill passes.
    scenario.commit = CommitProfile {
        per_commit_delay: Duration::from_millis(1),
        burst: None,
    };
    scenario.deadline = Duration::from_secs(60);
    let (_, report) = run_checked("fuzz_multi_peer_tight_budget", scenario, 128).await;

    // Ensure the bound is exercised.
    assert!(
        report.peak_budget_reserved >= byte_ceiling / 2,
        "the tight budget should bind under 8-peer contention (reserved {} of {} B)",
        report.peak_budget_reserved,
        byte_ceiling,
    );
}
