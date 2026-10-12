//! Aggregate serving bounds across many sessions that share one node capacity.

use super::*;
use zakura_chain::{block, serialization::ZcashDeserializeInto};
use zakura_test::{
    allocations::{in_isolated_process, ProcessMeasurement},
    vectors::BLOCK_MAINNET_1_BYTES,
};

const PEERS: usize = 16;
/// Requests each peer keeps outstanding, well within the advertised limit.
const WINDOW: u32 = 32;
/// Responses each peer completes, including its first window.
const TOTAL: u32 = 4 * WINDOW;
/// Height stride that gives each peer its own ranges.
const STRIDE: u32 = 1_000_000;

/// Serves one block per height to even peers and nothing to odd ones. Each
/// read waits for a gate permit and records how many reads run at once.
#[derive(Debug)]
struct LoadSource {
    block: Arc<block::Block>,
    gate: Arc<Semaphore>,
    running: Arc<[AtomicUsize; PEERS]>,
    peak: Arc<[AtomicUsize; PEERS]>,
    node_running: Arc<AtomicUsize>,
    node_peak: Arc<AtomicUsize>,
}

impl Source for LoadSource {
    fn read(&self, request: Read) -> BoxFuture<'static, Result<ReadResult, crate::BoxError>> {
        let peer = usize::try_from(request.start_height.0 / STRIDE).unwrap();
        let (block, gate) = (self.block.clone(), self.gate.clone());
        let (running, peak) = (self.running.clone(), self.peak.clone());
        let (node_running, node_peak) = (self.node_running.clone(), self.node_peak.clone());
        Box::pin(async move {
            let mut blocks = Vec::new();
            if request.lease.try_start() {
                let now = running[peer].fetch_add(1, Ordering::SeqCst) + 1;
                peak[peer].fetch_max(now, Ordering::SeqCst);
                let now = node_running.fetch_add(1, Ordering::SeqCst) + 1;
                node_peak.fetch_max(now, Ordering::SeqCst);
                gate.acquire().await?.forget();
                if peer % 2 == 0 {
                    blocks = (0..request.count)
                        .map(|offset| (Height(request.start_height.0 + offset), block.clone()))
                        .collect();
                }
                running[peer].fetch_sub(1, Ordering::SeqCst);
                node_running.fetch_sub(1, Ordering::SeqCst);
            }
            Ok(ReadResult {
                blocks,
                lease: request.lease,
            })
        })
    }
}

/// Yield until `done` holds. The bound counts scheduler passes, not time,
/// so it cannot advance the paused clock. A task waiting on a timer never
/// wakes while this runs, so such a wait fails here.
async fn yield_until(description: &str, mut done: impl FnMut() -> bool) {
    for _ in 0..1_000_000 {
        if done() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!(
        "{description} did not happen within the yield bound; serving may be waiting on a timer"
    );
}

/// Many sessions share one node capacity while half of them receive useful
/// bodies and half repeatedly receive empty responses. Execution slots bind
/// node-wide and per peer, every session is served before any finishes, all
/// complete without a tokio timer wait, and bookkeeping returns to its baseline.
///
/// The clock is paused and the test only yields, so a required tokio refill or
/// rate timer either never completes or advances the clock; both fail. Blocking
/// wall-clock sleeps and CPU cost are outside this check.
/// Encoding runs on blocking workers, so this measures allocations in an
/// isolated process. CPU use is bounded through execution slots rather than
/// measured directly.
#[test]
#[allow(clippy::print_stderr)]
fn concurrent_sessions_share_bounded_capacity_without_refill_waits() {
    in_isolated_process(
        "zakura::block_sync::regulated::session::tests::load::\
         concurrent_sessions_share_bounded_capacity_without_refill_waits",
        || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .max_blocking_threads(4)
                .enable_all()
                .start_paused(true)
                .build()
                .unwrap();
            runtime.block_on(async { tokio::task::spawn_blocking(|| ()).await.unwrap() });
            // Serving::new sizes execution for the largest possible response.
            let largest = Range {
                start: Height::MIN,
                count: MAX_BS_BLOCKS_PER_REQUEST,
            }
            .response_cap(MAX_BS_RESPONSE_BYTES)
            .output_bytes();
            let limits = sizing::serve_limits(largest, sizing::TARGET_RTT);
            let block: Arc<block::Block> =
                Arc::new(BLOCK_MAINNET_1_BYTES.zcash_deserialize_into().unwrap());
            let gate = Arc::new(Semaphore::new(0));
            let source = LoadSource {
                block,
                gate: gate.clone(),
                running: Arc::new(std::array::from_fn(|_| AtomicUsize::new(0))),
                peak: Arc::new(std::array::from_fn(|_| AtomicUsize::new(0))),
                node_running: Arc::new(AtomicUsize::new(0)),
                node_peak: Arc::new(AtomicUsize::new(0)),
            };
            let (peak, node_peak) = (source.peak.clone(), source.node_peak.clone());
            let (running, node_running) = (source.running.clone(), source.node_running.clone());
            let serving = Serving::new(Arc::new(source), &ZakuraBlockSyncConfig::default());
            assert!(PEERS * usize::try_from(WINDOW).unwrap() > limits.node_execution);
            assert!(usize::try_from(WINDOW).unwrap() > limits.peer_execution);

            let measurement = ProcessMeasurement::start();
            let (fixed, run) = runtime.block_on(async {
                let started = tokio::time::Instant::now();
                let cancel = CancellationToken::new();
                let mut sessions: Vec<_> = (0..PEERS)
                    .map(|index| {
                        let peer =
                            ZakuraPeerId::new(vec![u8::try_from(index).unwrap(); 32]).unwrap();
                        let (send, output) = framed_channel(8);
                        let session = serving.session(
                            &peer,
                            send,
                            cancel.clone(),
                            CancellationToken::new(),
                            Default::default(),
                        );
                        (session, output)
                    })
                    .collect();
                let fixed = measurement.snapshot();
                let base = |peer: usize| u32::try_from(peer).unwrap() * STRIDE + 1;
                let mut issued = [0_u32; PEERS];
                let mut fill = |peer: usize, session: &mut ServingSession| {
                    for _ in 0..WINDOW {
                        session.admit(Height(base(peer) + issued[peer]), 1).unwrap();
                        issued[peer] += 1;
                    }
                };

                // One peer alone cannot take more than its share of the node.
                fill(0, &mut sessions[0].0);
                yield_until("one peer fills its execution slots", || {
                    running[0].load(Ordering::SeqCst) >= limits.peer_execution
                })
                .await;
                for _ in 0..1_000 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(running[0].load(Ordering::SeqCst), limits.peer_execution);
                assert_eq!(
                    serving.capacity.node_execution_held(),
                    limits.peer_execution
                );

                for (peer, (session, _)) in sessions.iter_mut().enumerate().skip(1) {
                    fill(peer, session);
                }
                // Demand exceeds every execution limit, so reads plateau at the node limit.
                yield_until("reads fill the node execution slots", || {
                    node_running.load(Ordering::SeqCst) >= limits.node_execution
                })
                .await;
                for _ in 0..1_000 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(
                    serving.capacity.node_execution_held(),
                    limits.node_execution
                );
                assert_eq!(node_peak.load(Ordering::SeqCst), limits.node_execution);
                for peer in peak.iter() {
                    assert!(peer.load(Ordering::SeqCst) <= limits.peer_execution);
                }

                // Release reads and keep every peer's window full until it completes TOTAL.
                gate.add_permits(PEERS * usize::try_from(TOTAL).unwrap());
                let mut completed = [0_u32; PEERS];
                let mut bodies = [0_u32; PEERS];
                yield_until("every session completes its responses", || {
                    for (peer, (session, output)) in sessions.iter_mut().enumerate() {
                        while let Ok(frame) = output.try_recv() {
                            match frame.message_type {
                                3 => bodies[peer] += 1,
                                4 | 5 => {
                                    let useful = peer % 2 == 0;
                                    assert_eq!(frame.message_type == 4, useful);
                                    completed[peer] += 1;
                                    if issued[peer] < TOTAL {
                                        session
                                            .admit(Height(base(peer) + issued[peer]), 1)
                                            .unwrap();
                                        issued[peer] += 1;
                                    }
                                }
                                other => panic!("unexpected serving frame {other}"),
                            }
                        }
                    }
                    // No session may finish while another still waits for its first response.
                    if completed.contains(&TOTAL) {
                        assert!(completed.iter().all(|&count| count > 0), "{completed:?}");
                    }
                    completed.iter().all(|&count| count == TOTAL)
                })
                .await;
                for (peer, count) in bodies.iter().enumerate() {
                    assert_eq!(*count, if peer % 2 == 0 { TOTAL } else { 0 });
                }
                assert!(node_peak.load(Ordering::SeqCst) <= limits.node_execution);
                for peer in peak.iter() {
                    assert!(peer.load(Ordering::SeqCst) <= limits.peer_execution);
                }
                yield_until("capacity is released", || {
                    serving.capacity.node_execution_held() == 0
                        && serving.capacity.node_output_held() == 0
                })
                .await;
                for (session, _) in &mut sessions {
                    session.remove_completed();
                    assert!(session.ranges.is_empty());
                }
                assert_eq!(
                    tokio::time::Instant::now(),
                    started,
                    "serving never waited on a timer"
                );
                let run = measurement.snapshot();
                cancel.cancel();
                drop(sessions);
                (fixed, run)
            });
            // Joining the runtime drains worker-owned allocations before the final snapshot.
            drop((runtime, serving));
            let cleaned = measurement.snapshot();
            drop(measurement);
            eprintln!("aggregate serving fixed={fixed:?} run={run:?} cleaned={cleaned:?}");
            // Per-session and per-request ceilings follow the lifecycle regression.
            // Each queued response may also hold one encoded body and its frame.
            const FIXED: usize = 16 * 1024;
            const WORKERS: usize = 128 * 1024;
            let requests = PEERS * usize::try_from(WINDOW).unwrap();
            let body = BLOCK_MAINNET_1_BYTES.len() + 1024;
            assert!(fixed.peak_live_bytes <= PEERS * FIXED, "{fixed:?}");
            assert!(
                run.peak_live_bytes <= PEERS * FIXED + WORKERS + requests * (160 + body),
                "{run:?}"
            );
            assert!(cleaned.retained_bytes <= FIXED, "{cleaned:?}");
            // A disabled observer must not make the upper bounds vacuous.
            assert!(run.peak_live_bytes >= requests * 64, "{run:?}");
        },
    );
}
