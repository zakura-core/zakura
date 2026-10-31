//! Manual real-runtime comparison of the composed resource owners.
//!
//! Nothing here runs by default, and no result is recorded in the tree. Run it on a
//! release build:
//!
//! ```text
//! ZAKURA_COMPARISON_PROBE=1 cargo test --release -p zakura-network --lib \
//!     node_envelope::comparison -- --ignored --nocapture --test-threads=1
//! ```
//!
//! # Baseline
//!
//! Each row compares the composed limits with [`baseline_limits`] in one process: the
//! same owners, peers, frame sizes, delays and stalls, with windows that never bind.
//! That isolates what the bounds cost. The cost of the owners themselves needs a
//! second comparison against the merge base with `main`, where these owners don't
//! exist, through the probes that both builds have:
//!
//! - `maximum_sessions_continuous_serving_probe` for serving throughput and memory,
//! - `request_bookkeeping_scaling` for per-request bookkeeping,
//! - `getblocks_storage_read_probe` for storage reads and storage stalls.
//!
//! `partial_receipts_and_reset_batch_scaling` covers partial receipts and reset batches.
//!
//! # Tolerances
//!
//! [`TOLERANCES`] states what a row may lose against its baseline, and the test fails
//! beyond it. The values are declarations to confirm against a first run. No run has
//! measured them yet.
//!
//! # Not measured here
//!
//! Lock hold and contention and CAS retries have no counters, because the production
//! owners keep test evidence out of their hot paths. Measure them from outside the
//! process, for example with `perf lock` and `perf stat`, on this same test.

use std::{
    collections::VecDeque,
    future::Future,
    pin::pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};
use zakura_node_services::block_memory::{Admission, DecodedBlockPool};
use zakura_test::allocations::measure;

use crate::zakura::{
    transport::{NodeResources, ResourceEnvelope, StreamResources, TransportLimits},
    DEFAULT_ZAKURA_MAX_CONNECTIONS, DEFAULT_ZAKURA_RECEIVE_WINDOW,
};

/// The longest one scenario may run before the probe fails on lost progress.
const SCENARIO_DEADLINE: Duration = Duration::from_secs(60);
/// Frames each peer moves in one scenario.
const FRAMES_PER_PEER: u64 = 256;
/// Peer counts: below and at the default connection cap.
const PEERS: [usize; 2] = [16, DEFAULT_ZAKURA_MAX_CONNECTIONS];
/// Object sizes: a small message and a block chunk.
const FRAME_BYTES: [u64; 2] = [1 << 10, 1 << 20];
/// How long a consumer retains a frame: a local hand-off, then a stalled store.
const CONSUMER_DELAYS: [Duration; 2] = [Duration::ZERO, Duration::from_millis(5)];

/// What a row may lose against its baseline.
struct Tolerances {
    /// The least useful throughput, as a share of the baseline's.
    throughput: f64,
    /// The largest p99 completion latency, as a multiple of the baseline's, above
    /// [`LATENCY_FLOOR`].
    p99_latency: f64,
    /// The largest allocation count, as a multiple of the baseline's.
    allocations: f64,
    /// The largest poll count, as a multiple of the baseline's. A poll stands in for
    /// a wakeup.
    polls: f64,
}

const TOLERANCES: Tolerances = Tolerances {
    throughput: 0.95,
    p99_latency: 1.10,
    allocations: 1.10,
    polls: 1.25,
};

/// Latency below this is scheduler noise, so the p99 comparison ignores it.
const LATENCY_FLOOR: Duration = Duration::from_millis(1);

/// The transport limits a row measures.
fn composed_limits(peers: usize) -> TransportLimits {
    TransportLimits {
        receive_window: DEFAULT_ZAKURA_RECEIVE_WINDOW,
        send_window: u64::from(DEFAULT_ZAKURA_RECEIVE_WINDOW),
        max_connections: peers,
        max_pending_handshakes: 1,
        max_open_streams: 16,
        max_frame_bytes: 1 << 21,
    }
}

/// The same owners with windows that never bind.
fn baseline_limits(peers: usize) -> TransportLimits {
    TransportLimits {
        receive_window: u32::MAX,
        send_window: u64::from(u32::MAX),
        ..composed_limits(peers)
    }
}

/// One workload, run against both limits.
#[derive(Clone, Copy, Debug)]
enum Scenario {
    /// Every peer moves frames through a consumer that retains them.
    Throughput,
    /// A quarter of the peers stop reading. The others must still finish.
    BlockedOutput,
    /// Peers reconnect after every frame, so owners keep entering and leaving.
    ReconnectChurn,
    /// Peers send frames with empty payloads as fast as the owners admit them.
    EmptyMessageFlood,
    /// One family holds the node budgets while others move floor frames.
    MixedFamilySaturation,
}

const SCENARIOS: [Scenario; 5] = [
    Scenario::Throughput,
    Scenario::BlockedOutput,
    Scenario::ReconnectChurn,
    Scenario::EmptyMessageFlood,
    Scenario::MixedFamilySaturation,
];

/// What one peer's task reports through.
#[derive(Debug, Default)]
struct Tally {
    bytes: AtomicU64,
    peak_held: AtomicU64,
    polls: AtomicU64,
    latencies_us: Mutex<Vec<u32>>,
}

impl Tally {
    /// Admit one frame, recording its latency, the polls it took, and the node's
    /// attributed bytes afterwards.
    async fn timed<T>(&self, node: &NodeResources, admit: impl Future<Output = T>) -> T {
        let started = Instant::now();
        let mut admit = pin!(admit);
        let admitted = std::future::poll_fn(|context| {
            self.polls.fetch_add(1, Ordering::Relaxed);
            admit.as_mut().poll(context)
        })
        .await;
        let latency = u32::try_from(started.elapsed().as_micros()).unwrap_or(u32::MAX);
        self.latencies_us
            .lock()
            .expect("no recording panics")
            .push(latency);
        let usage = node.usage();
        self.peak_held
            .fetch_max(usage.raw_bytes + usage.output_bytes, Ordering::Relaxed);
        admitted
    }
}

/// One scenario's measurement.
#[derive(Clone, Copy, Debug)]
struct Row {
    bytes_per_second: f64,
    p99_latency: Duration,
    peak_held: u64,
    polls: u64,
    allocations: usize,
}

/// Move `frames` frames on one stream to a consumer that retains each for `delay`.
///
/// The consumer is its own task, as in production: a reader that waits for budget
/// waits for the consumer's release, never for itself.
async fn pipe(
    node: NodeResources,
    stream: StreamResources,
    frames: u64,
    bytes: u64,
    delay: Duration,
    tally: Arc<Tally>,
) {
    let (retain, mut retained) = tokio::sync::mpsc::unbounded_channel();
    let consumer = {
        let tally = tally.clone();
        zakura_rt::task::spawn(async move {
            while let Some((grant, until)) = retained.recv().await {
                zakura_rt::time::sleep_until(until).await;
                drop(grant);
                tally.bytes.fetch_add(bytes, Ordering::Relaxed);
            }
        })
    };
    for _ in 0..frames {
        let grant = tally.timed(&node, stream.raw.admit(bytes)).await;
        retain
            .send((grant, zakura_rt::time::Instant::now() + delay))
            .expect("the consumer outlives its sender");
        zakura_rt::task::yield_now().await;
    }
    drop(retain);
    consumer.await.expect("the consumer does not panic");
}

/// Run `scenario` to its end on the current runtime.
async fn drive(
    scenario: Scenario,
    limits: TransportLimits,
    peers: usize,
    bytes: u64,
    delay: Duration,
    tally: Arc<Tally>,
) {
    let node = NodeResources::new(ResourceEnvelope::new(limits));
    let mut tasks = zakura_rt::task::JoinSet::new();
    match scenario {
        Scenario::Throughput => {
            for _ in 0..peers {
                let stream = node.connection().stream();
                let (node, tally) = (node.clone(), tally.clone());
                tasks.spawn(pipe(node, stream, FRAMES_PER_PEER, bytes, delay, tally));
            }
        }
        Scenario::BlockedOutput => {
            // These peers fill their connection budgets and hold them to the end.
            let blocked: Vec<_> = (0..peers / 4)
                .map(|_| {
                    let stream = node.connection().stream();
                    let held = u64::from(limits.receive_window) * 2;
                    let grant = stream.raw.try_admit(held);
                    (stream, grant)
                })
                .collect();
            for _ in peers / 4..peers {
                let stream = node.connection().stream();
                let (node, tally) = (node.clone(), tally.clone());
                tasks.spawn(pipe(node, stream, FRAMES_PER_PEER, bytes, delay, tally));
            }
            while tasks.join_next().await.is_some() {}
            drop(blocked);
        }
        Scenario::ReconnectChurn => {
            for _ in 0..peers {
                let (node, tally) = (node.clone(), tally.clone());
                tasks.spawn(async move {
                    for _ in 0..FRAMES_PER_PEER {
                        let stream = node.connection().stream();
                        let grant = tally.timed(&node, stream.raw.admit(bytes)).await;
                        drop((grant, stream));
                        tally.bytes.fetch_add(bytes, Ordering::Relaxed);
                        zakura_rt::task::yield_now().await;
                    }
                });
            }
        }
        Scenario::EmptyMessageFlood => {
            for _ in 0..peers {
                let stream = node.connection().stream();
                let (node, tally) = (node.clone(), tally.clone());
                let empty = crate::zakura::transport::frame_wire_bytes(0);
                tasks.spawn(pipe(node, stream, FRAMES_PER_PEER, empty, delay, tally));
            }
        }
        Scenario::MixedFamilySaturation => {
            let holds = node.hold_node_budgets();
            for _ in 0..peers {
                let stream = node.connection().stream();
                let (node, tally) = (node.clone(), tally.clone());
                tasks.spawn(async move {
                    for _ in 0..FRAMES_PER_PEER {
                        // Only the stream's floor slot funds this frame.
                        let grant = tally.timed(&node, stream.output.admit(bytes)).await;
                        drop(grant);
                        tally.bytes.fetch_add(bytes, Ordering::Relaxed);
                        zakura_rt::task::yield_now().await;
                    }
                });
            }
            while tasks.join_next().await.is_some() {}
            drop(holds);
        }
    }
    while tasks.join_next().await.is_some() {}
}

/// Measure one row: throughput and latency on all workers, and allocations on one
/// thread, because [`measure`] counts only its own thread.
fn run_row(
    scenario: Scenario,
    limits: TransportLimits,
    peers: usize,
    bytes: u64,
    delay: Duration,
) -> Row {
    let bounded = |run: Arc<Tally>| async move {
        zakura_rt::time::timeout(
            SCENARIO_DEADLINE,
            drive(scenario, limits, peers, bytes, delay, run),
        )
        .await
        .unwrap_or_else(|_| panic!("{scenario:?} lost progress with {peers} peers"));
    };

    let parallel = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("the runtime builds");
    let tally = Arc::new(Tally::default());
    let started = Instant::now();
    parallel.block_on(bounded(tally.clone()));
    let elapsed = started.elapsed();

    let single = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the runtime builds");
    let (_, allocations) = measure(|| single.block_on(bounded(Arc::new(Tally::default()))));

    let mut latencies = tally
        .latencies_us
        .lock()
        .expect("no recording panics")
        .clone();
    latencies.sort_unstable();
    let p99 = latencies
        .get(latencies.len() * 99 / 100)
        .copied()
        .unwrap_or(0);
    Row {
        // Float conversions lose nothing that matters below 2^53 bytes.
        bytes_per_second: tally.bytes.load(Ordering::Relaxed) as f64 / elapsed.as_secs_f64(),
        p99_latency: Duration::from_micros(u64::from(p99)),
        peak_held: tally.peak_held.load(Ordering::Relaxed),
        polls: tally.polls.load(Ordering::Relaxed),
        allocations: allocations.requests,
    }
}

/// Every problem with `composed` against `baseline`, under [`TOLERANCES`].
fn tolerance_problems(composed: &Row, baseline: &Row, bound: u128) -> Vec<String> {
    let mut problems = Vec::new();
    if composed.bytes_per_second < baseline.bytes_per_second * TOLERANCES.throughput {
        problems.push("throughput".to_owned());
    }
    if composed.p99_latency > LATENCY_FLOOR
        && composed.p99_latency.as_secs_f64()
            > baseline.p99_latency.as_secs_f64() * TOLERANCES.p99_latency
    {
        problems.push("p99 latency".to_owned());
    }
    // Float conversions lose nothing that matters at these counts.
    if composed.allocations as f64 > baseline.allocations as f64 * TOLERANCES.allocations {
        problems.push("allocations".to_owned());
    }
    if composed.polls as f64 > baseline.polls as f64 * TOLERANCES.polls {
        problems.push("wakeups".to_owned());
    }
    if u128::from(composed.peak_held) > bound {
        problems.push(format!("peak {} above bound {bound}", composed.peak_held));
    }
    problems
}

/// Compare every scenario under the composed and baseline limits.
#[test]
#[ignore = "manual real-runtime comparison; requires ZAKURA_COMPARISON_PROBE=1"]
#[allow(clippy::print_stderr)] // Report each row with --nocapture.
fn composed_owners_stay_within_their_tolerances() {
    if std::env::var_os("ZAKURA_COMPARISON_PROBE").is_none() {
        return;
    }
    let mut problems = Vec::new();
    for scenario in SCENARIOS {
        for peers in PEERS {
            for bytes in FRAME_BYTES {
                for delay in CONSUMER_DELAYS {
                    let limits = composed_limits(peers);
                    let composed = run_row(scenario, limits, peers, bytes, delay);
                    let baseline = run_row(scenario, baseline_limits(peers), peers, bytes, delay);
                    eprintln!(
                        "{scenario:?} peers={peers} bytes={bytes} delay={delay:?} \
                         composed={composed:?} baseline={baseline:?}"
                    );
                    let bound = ResourceEnvelope::new(limits).retained_frame_bytes();
                    for problem in tolerance_problems(&composed, &baseline, bound) {
                        problems.push(format!(
                            "{scenario:?} peers={peers} bytes={bytes} delay={delay:?}: {problem}"
                        ));
                    }
                }
            }
        }
    }
    assert!(problems.is_empty(), "outside tolerance: {problems:#?}");
}

/// What decoded admission costs when it refuses: each refusal keeps the raw bytes, so
/// a retry decodes the block again.
///
/// The decode count must stay equal to the admitted count while the pipeline limit
/// doesn't bind, and each retry must wait for a capacity release, not spin.
#[test]
#[ignore = "manual real-runtime comparison; requires ZAKURA_COMPARISON_PROBE=1"]
#[allow(clippy::print_stderr)] // Report each row with --nocapture.
fn failed_decoded_admission_keeps_decodes_within_tolerance() {
    /// Decodes per admitted block a refusing pool may spend.
    const DECODE_TOLERANCE: f64 = 2.0;
    const BLOCKS: u64 = 4096;
    /// The pool holds at most this many blocks' bytes before a consumer releases one.
    const RESIDENT: u64 = 16;

    if std::env::var_os("ZAKURA_COMPARISON_PROBE").is_none() {
        return;
    }
    let decode = || -> Block {
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .expect("the test vector is a valid block")
    };
    let block_bytes = decode().attributed_memory_size_bytes();
    let count = std::num::NonZeroU64::new(BLOCKS).expect("the block count is positive");
    let started = Instant::now();
    let mut decodes = 0_u64;
    let pool = DecodedBlockPool::for_pipeline(count, RESIDENT * block_bytes);
    let mut retained = VecDeque::new();
    let mut admitted = 0;
    while admitted < BLOCKS {
        decodes += 1;
        match pool.try_admit(decode(), Admission::WithinLimit) {
            Ok(block) => {
                retained.push_back(block);
                admitted += 1;
            }
            // The refusal drops this decode. The oldest block's release funds the retry.
            Err(_) => drop(retained.pop_front()),
        }
        assert!(
            started.elapsed() < SCENARIO_DEADLINE,
            "admission lost progress"
        );
    }
    // Float conversions lose nothing that matters at these counts.
    let per_block = decodes as f64 / admitted as f64;
    eprintln!("decodes={decodes} admitted={admitted} per_block={per_block:.2}");
    assert!(
        per_block <= DECODE_TOLERANCE,
        "{per_block} decodes per block"
    );
}
