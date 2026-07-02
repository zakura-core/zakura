//! Named block-sync fuzzer scenarios + invariant assertions.
//!
//! Each test drives the real reactor through a distinct adversarial shape and asserts
//! the core invariants (no stall, contiguous/correct commit, bounded in-flight). They
//! emit the standard JSONL; run with `ZAKURA_TEST_TRACE=keep` and point the analysis
//! scripts at `target/zakura-traces/<name>/node-00` to inspect a run.

use std::time::Duration;

use zebra_chain::block;

use super::{
    assert_core_invariants, fuzz_config, invariant_report, run_scenario, run_trace,
    CommitBurstStall, CommitProfile, FuzzOutcome, IdleGap, InvariantReport, LatencyDist, PeerSpec,
    Scenario, ServeProfile, TipEvent, TipEventKind,
};
use crate::zakura::ZakuraBlockSyncConfig;

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
    let blocks = 1000;
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
    scenario.timeline = vec![
        TipEvent {
            at: Duration::from_millis(60),
            kind: TipEventKind::GrowTo(block::Height(300)),
        },
        TipEvent {
            at: Duration::from_millis(140),
            kind: TipEventKind::GrowTo(block::Height(700)),
        },
        TipEvent {
            at: Duration::from_millis(200),
            kind: TipEventKind::HeaderReanchor(block::Height(500)),
        },
        TipEvent {
            at: Duration::from_millis(280),
            kind: TipEventKind::GrowTo(block::Height(1000)),
        },
    ];
    scenario.deadline = Duration::from_secs(60);
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

/// Commit stall: the mock commit pipeline drains slowly and in bursts while fast peers
/// keep serving. This is the `BLOCKSYNC_BYTE_CWND_PLAN` step-3 system check — the apply
/// backlog must be bounded **only** by the byte budget (download parks on the memory
/// ceiling, not on a commit-coupled throttle), and the verified tip must resume the
/// instant each stall clears so the run still reaches the target.
///
/// `run_checked` already asserts the target is reached (vtip resumes; no commit-induced
/// wedge). On top of that we assert the peak reserved bytes stayed within the configured
/// ceiling (the queue did not grow toward the full chain) yet the ceiling was actually
/// exercised (the stall created real backpressure — the bound is not vacuous).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_commit_stall() {
    let blocks = 400;
    let body_bytes = 32 * 1024usize;
    // A small resident download budget so the commit stall makes it bind: the chain is
    // ~12.8 MB (400 × 32 KiB) against a 2 MiB ceiling, so the budget must recycle ~6× and
    // download genuinely waits on commit. Kept below the request-count cap's byte
    // equivalent (2 peers × 64 reqs × 32 KiB = 4 MiB) so the *byte budget*, not the
    // request count, is the binding constraint. (The fuzzer reactor path does not call
    // `validate()`, which would otherwise require a full checkpoint range; the synthetic
    // bodies are tiny and the mock committer needs no checkpoint batch, so a sub-floor
    // ceiling is the right knob to exercise byte-budget backpressure here.)
    let byte_ceiling: u64 = 2 * 1024 * 1024;
    let config = ZakuraBlockSyncConfig {
        max_inflight_block_bytes: byte_ceiling,
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
    // Steady 1 ms/commit plus a 120 ms burst every 40 commits — a slow, sawtoothing drain
    // that holds the byte budget full between bursts without ever permanently wedging.
    scenario.commit = CommitProfile {
        per_commit_delay: Duration::from_millis(1),
        burst: Some(CommitBurstStall {
            every_commits: 40,
            duration: Duration::from_millis(120),
        }),
    };
    scenario.deadline = Duration::from_secs(60);
    let (_, report) = run_checked("fuzz_commit_stall", scenario, 64).await;

    // The byte budget is the only bound on the apply backlog: peak reserved bytes stay
    // within the ceiling plus a small floor-bypass / request-boundary margin — i.e. the
    // queue did not grow toward the 12.8 MB chain despite the commit stall.
    let margin = (body_bytes as u64).saturating_mul(16);
    let bound = byte_ceiling.saturating_add(margin);
    assert!(
        report.peak_budget_reserved <= bound,
        "reserved bytes {} must stay within the {} B memory ceiling (+{} B margin); the \
         apply backlog is bounded by the byte budget, not unbounded by the commit stall",
        report.peak_budget_reserved,
        byte_ceiling,
        margin,
    );
    // Non-vacuous: the stall actually filled the budget (otherwise the bound proves
    // nothing about backpressure).
    assert!(
        report.peak_budget_reserved >= byte_ceiling / 2,
        "the commit stall should have created real byte-budget backpressure (reserved \
         {} of the {} B ceiling)",
        report.peak_budget_reserved,
        byte_ceiling,
    );
    tracing::info!(
        peak_budget_reserved = report.peak_budget_reserved,
        final_budget_reserved = report.final_budget_reserved,
        byte_ceiling,
        "commit_stall byte-budget backpressure observation",
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

/// Many peers racing against a tight global byte budget: every per-peer routine reserves
/// against the one shared `ByteBudget`, so this stresses the concurrent reservation path
/// end-to-end. A steady slow commit keeps the shared budget full so it actually binds
/// while eight routines reserve concurrently. `assert_core` asserts
/// `peak_budget_reserved` never exceeds the configured ceiling (the global memory bound
/// under multi-peer contention — the spec's "concurrent reservations MUST NOT
/// over-commit"); here we additionally assert the ceiling was genuinely approached, so
/// that bound is not vacuous.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_multi_peer_tight_budget() {
    let blocks = 400;
    let body_bytes = 32 * 1024usize;
    // ~12.8 MB chain against a 3 MiB ceiling ⇒ the shared budget recycles ~4× and binds.
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
    // A steady slow commit so the budget stays full and binds; instant commit would keep
    // it nearly empty and make the bound vacuous.
    scenario.commit = CommitProfile {
        per_commit_delay: Duration::from_millis(1),
        burst: None,
    };
    scenario.deadline = Duration::from_secs(60);
    let (_, report) = run_checked("fuzz_multi_peer_tight_budget", scenario, 128).await;

    // Non-vacuous: the tight ceiling was genuinely approached under 8-peer contention, so
    // `assert_core`'s `peak_budget_reserved <= max_inflight_block_bytes` proves a real
    // bound rather than an idle budget.
    assert!(
        report.peak_budget_reserved >= byte_ceiling / 2,
        "the tight budget should bind under 8-peer contention (reserved {} of {} B)",
        report.peak_budget_reserved,
        byte_ceiling,
    );
}
