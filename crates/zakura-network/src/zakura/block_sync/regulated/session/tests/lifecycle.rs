//! Pin serving bookkeeping across completion, idling and overlapping session cleanup.

use super::*;
use zakura_test::allocations::{in_isolated_process, AllocationStats, ProcessMeasurement};

/// Every snapshot shares one allocation history, including frees on encoding workers.
#[derive(Debug)]
struct LifecycleSamples {
    fixed: AllocationStats,
    admitted: AllocationStats,
    idle: AllocationStats,
    cancelled: AllocationStats,
    replacement: AllocationStats,
    cleaned: AllocationStats,
}

/// Check production ownership transitions with every default commitment in one session.
/// Isolation excludes unrelated tests, while the observer includes blocking encoding workers.
#[test]
#[allow(clippy::print_stderr)]
fn compact_session_lifecycle_peak() {
    in_isolated_process(
        "zakura::block_sync::regulated::session::tests::lifecycle::compact_session_lifecycle_peak",
        || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .max_blocking_threads(4)
                .enable_all()
                .build()
                .unwrap();
            // Initialize a worker before measuring. Additional worker costs still count.
            runtime.block_on(async { tokio::task::spawn_blocking(|| ()).await.unwrap() });
            let gate = Arc::new(Semaphore::new(0));
            let source = Arc::new(ControlledSource {
                default_gate: Some(gate.clone()),
                ..Default::default()
            });
            let serving = Serving::new(source, &ZakuraBlockSyncConfig::default());
            let peer = ZakuraPeerId::new(vec![63; 32]).unwrap();
            let count =
                super::super::serving_max_inflight_requests(&ZakuraBlockSyncConfig::default()) * 2;
            let measurement = ProcessMeasurement::start();
            let (fixed, admitted, idle, cancelled, replacement) = runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(90), async {
                    let cancel = CancellationToken::new();
                    let (send, mut output) = framed_channel(4);
                    let mut session = serving.session(
                        &peer,
                        send,
                        cancel.clone(),
                        CancellationToken::new(),
                        Default::default(),
                    );
                    let fixed = measurement.snapshot();
                    for height in 1..=count {
                        session.admit(Height(height), 1).unwrap();
                    }
                    let admitted = measurement.snapshot();
                    gate.add_permits(usize::try_from(count).unwrap());
                    for height in 1..=count {
                        assert!(
                            matches!(super::super::super::tests::receive(&mut output).await,
                            super::super::super::wire::Message::RangeUnavailable(range)
                                if range.start == Height(height))
                        );
                    }
                    while serving.capacity.node_execution_held() != 0 {
                        tokio::task::yield_now().await;
                    }
                    // No further admission means no completion drain. This retained state
                    // is intentional and is charged within the peak session allowance.
                    assert_eq!(session.ranges.len(), usize::try_from(count).unwrap());
                    let idle = measurement.snapshot();
                    // The first new admission drains every old completion before overlap checks.
                    for height in 1..=count {
                        session.admit(Height(height), 1).unwrap();
                    }
                    cancel.cancel();
                    drop((session, output, cancel));
                    let cancelled = measurement.snapshot();
                    // No yield here: old queued jobs are still owned by their cancelled dispatch.
                    let cancel = CancellationToken::new();
                    let (send, output) = framed_channel(4);
                    let mut session = serving.session(
                        &peer,
                        send,
                        cancel.clone(),
                        CancellationToken::new(),
                        Default::default(),
                    );
                    for height in 1..=count {
                        session.admit(Height(height), 1).unwrap();
                    }
                    let replacement = measurement.snapshot();
                    cancel.cancel();
                    drop((session, output, cancel));
                    gate.add_permits(128);
                    for _ in 0..4 {
                        tokio::task::yield_now().await;
                    }
                    while serving.capacity.node_execution_held() != 0 {
                        tokio::task::yield_now().await;
                    }
                    (fixed, admitted, idle, cancelled, replacement)
                })
                .await
                .unwrap()
            });
            // Joining the runtime drains worker-owned allocations before the final snapshot.
            drop((runtime, serving));
            let samples = LifecycleSamples {
                fixed,
                admitted,
                idle,
                cancelled,
                replacement,
                cleaned: measurement.snapshot(),
            };
            drop(measurement);
            eprintln!("compact session lifecycle count={count}: {samples:#?}");
            assert_lifecycle_budget(usize::try_from(count).unwrap(), &samples);
        },
    );
}

/// Bound idle retention and replacement overlap separately from ordinary admission.
/// These are bookkeeping regression limits, not a budget for storage or QUIC buffers.
fn assert_lifecycle_budget(count: usize, samples: &LifecycleSamples) {
    const FIXED: usize = 16 * 1024;
    const WORKERS: usize = 128 * 1024;
    assert!(samples.fixed.peak_live_bytes <= FIXED, "{samples:?}");
    assert!(
        samples.admitted.peak_live_bytes <= FIXED + 160 * count,
        "{samples:?}"
    );
    assert!(
        samples.idle.retained_bytes <= FIXED + WORKERS + 96 * count,
        "{samples:?}"
    );
    assert!(
        samples.idle.peak_live_bytes <= FIXED + WORKERS + 160 * count,
        "{samples:?}"
    );
    assert!(
        samples.cancelled.peak_live_bytes <= FIXED + WORKERS + 160 * count,
        "{samples:?}"
    );
    assert!(
        samples.cancelled.retained_bytes <= FIXED + WORKERS + 64 * count,
        "{samples:?}"
    );
    assert!(
        samples.replacement.peak_live_bytes <= 2 * FIXED + WORKERS + 224 * count,
        "{samples:?}"
    );
    assert!(
        samples.cleaned.peak_live_bytes <= 2 * FIXED + WORKERS + 224 * count,
        "{samples:?}"
    );
    assert!(samples.cleaned.retained_bytes <= FIXED, "{samples:?}");
    // A disabled or broken observer must not turn every upper bound into a vacuous pass.
    assert!(samples.admitted.retained_bytes >= 64 * count, "{samples:?}");
    assert!(samples.idle.retained_bytes >= 32 * count, "{samples:?}");
    assert!(
        samples.replacement.retained_bytes > samples.admitted.retained_bytes,
        "{samples:?}"
    );
}
