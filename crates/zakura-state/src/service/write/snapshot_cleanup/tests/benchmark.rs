//! Synthetic publication benchmark using actual chain indexes, excluding fixture construction.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use tokio::sync::watch;

use super::{snapshot, Network, NonFinalizedState, SnapshotCleanup};

#[derive(Default)]
struct Handoffs {
    background: Arc<AtomicU64>,
    inline: Arc<AtomicU64>,
}

impl Recorder for Handoffs {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        match key.name() {
            "state.snapshot_cleanup.offloaded" => Counter::from_arc(self.background.clone()),
            "state.snapshot_cleanup.inline" => Counter::from_arc(self.inline.clone()),
            _ => Counter::noop(),
        }
    }
    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }
    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

fn owned_copy(state: &NonFinalizedState) -> NonFinalizedState {
    let mut copy = NonFinalizedState::new(&Network::Mainnet);
    for chain in state.chain_iter() {
        copy.insert_test_chain(Arc::new((**chain).clone()));
    }
    copy
}

#[test]
#[ignore = "optimized publication benchmark; run separately with --nocapture --test-threads=1"]
#[allow(clippy::print_stdout)]
#[allow(
    clippy::assertions_on_constants,
    reason = "reject debug benchmarks at execution time"
)]
fn compare_snapshot_publication() {
    assert!(
        !cfg!(debug_assertions),
        "use --profile ci-tests or --release"
    );
    let mode = std::env::var("ZAKURA_SNAPSHOT_BENCH_MODE").unwrap_or_else(|_| "both".into());
    let rounds = 20;
    for forks in [1, 5, 10] {
        // 1,000 block records and 10,000 transaction/UTXO entries per chain. The
        // indexes are real types, but these are not replayed valid chain histories.
        let seed = snapshot(forks, 10_000);
        for retained_reader in [false, true] {
            for burst in [1, 8] {
                // Alternate order between cases to reduce systematic warm-cache bias.
                let order = if burst == 1 {
                    [false, true]
                } else {
                    [true, false]
                };
                for deferred in order {
                    let label = if deferred { "deferred" } else { "inline" };
                    if mode != "both" && mode != label {
                        continue;
                    }
                    let counters = Handoffs::default();
                    let cleanup = deferred.then(SnapshotCleanup::new);
                    let mut latency = Vec::new();
                    let mut drained = Vec::new();
                    metrics::with_local_recorder(&counters, || {
                        for round in 0..rounds + 3 {
                            let mut batch = Vec::new();
                            for _ in 0..burst {
                                let old = owned_copy(&seed);
                                let reader = retained_reader.then(|| old.clone());
                                let (sender, receiver) = watch::channel(old);
                                batch.push((sender, receiver, reader, owned_copy(&seed)));
                            }
                            let start = Instant::now();
                            for (sender, _, _, new) in &mut batch {
                                let new = std::mem::replace(
                                    new,
                                    NonFinalizedState::new(&Network::Mainnet),
                                );
                                let published = Instant::now();
                                if deferred {
                                    cleanup.as_ref().unwrap().publish(sender, new);
                                } else {
                                    sender.send(new).unwrap();
                                }
                                if round >= 3 {
                                    latency.push(published.elapsed().as_nanos());
                                }
                            }
                            // Rendezvous after the burst includes all previously offloaded
                            // disposal. Lower response latency must not hide unfinished work.
                            if deferred {
                                cleanup
                                    .as_ref()
                                    .unwrap()
                                    .sender
                                    .as_ref()
                                    .unwrap()
                                    .send(NonFinalizedState::new(&Network::Mainnet))
                                    .unwrap();
                            }
                            if round >= 3 {
                                drained.push(start.elapsed().as_nanos());
                            }
                            // Includes last-reader destruction, outside publication timing.
                            drop(batch);
                        }
                    });
                    drop(cleanup);
                    latency.sort_unstable();
                    drained.sort_unstable();
                    println!("mode={label} forks={forks} retained_reader={retained_reader} burst={burst} samples={} publication_median_ns={} publication_p95_ns={} burst_drained_median_ns={} offloaded={} inline={}",
                        latency.len(), latency[latency.len()/2], latency[latency.len()*95/100],
                        drained[drained.len()/2], counters.background.load(Ordering::Relaxed),
                        counters.inline.load(Ordering::Relaxed));
                }
            }
        }
    }
}

/// Exercise real contextual commits and in-memory finalization against an ephemeral DB.
/// Fake children are preverified fixtures; this excludes semantic verification, DB finalization,
/// mining RPC, and network delivery, so it is not a miner turnaround benchmark.
#[test]
#[ignore = "optimized contextual state replay; run separately with --nocapture --test-threads=1"]
#[allow(clippy::print_stdout)]
#[allow(
    clippy::assertions_on_constants,
    reason = "reject debug benchmarks at execution time"
)]
fn compare_snapshot_state_replay() {
    use crate::{
        arbitrary::Prepare,
        service::{finalized_state::FinalizedState, ChainTipSender},
        tests::FakeChainHelper,
        Config,
    };
    use zakura_chain::{amount::NonNegative, value_balance::ValueBalance};

    assert!(
        !cfg!(debug_assertions),
        "use --profile ci-tests or --release"
    );
    let network = Network::Mainnet;
    let finalized = FinalizedState::new(&Config::ephemeral(), &network).unwrap();
    finalized.set_finalized_value_pool(ValueBalance::<NonNegative>::fake_populated_pool());
    // Before Heartwood, these existing test fixtures need no populated history tree.
    let mut parent = Arc::new(network.test_block(653_599, 583_999).unwrap());
    let mut seed = NonFinalizedState::new(&network);
    seed.commit_new_chain(parent.clone().prepare(), &finalized)
        .unwrap();
    for _ in 1..1_000 {
        parent = parent.make_fake_child();
        seed.commit_block(parent.clone().prepare(), &finalized)
            .unwrap();
    }
    let blocks: Vec<_> = (0..64)
        .map(|_| {
            parent = parent.make_fake_child();
            parent.clone().prepare()
        })
        .collect();

    for round in 0..6 {
        let order = if round % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        };
        for deferred in order {
            let mut live = seed.clone();
            let (sender, receiver) = watch::channel(live.clone());
            let (mut tips, _latest, _changes) = ChainTipSender::new(None, &network);
            let cleanup = if deferred {
                SnapshotCleanup::new()
            } else {
                SnapshotCleanup {
                    sender: None,
                    worker: None,
                }
            };
            let counters = Handoffs::default();
            let mut responses = Vec::new();
            let start = Instant::now();
            metrics::with_local_recorder(&counters, || {
                for block in &blocks {
                    let response = Instant::now();
                    live.commit_block(block.clone(), &finalized).unwrap();
                    super::super::super::update_latest_chain_channels(
                        &live, &mut tips, &sender, None, &cleanup,
                    );
                    responses.push(response.elapsed().as_nanos());
                    // Keep the real 1,000-block window. Disk commit is deliberately excluded.
                    live.finalize();
                    super::super::super::update_latest_chain_channels(
                        &live, &mut tips, &sender, None, &cleanup,
                    );
                    assert_eq!(receiver.borrow().best_tip(), live.best_tip());
                }
            });
            drop(cleanup);
            let drained = start.elapsed().as_nanos();
            responses.sort_unstable();
            println!("replay round={round} deferred={deferred} blocks={} response_median_ns={} response_p95_ns={} total_drained_ns={drained} offloaded={} inline={}",
                blocks.len(), responses[responses.len()/2], responses[responses.len()*95/100],
                counters.background.load(Ordering::Relaxed), counters.inline.load(Ordering::Relaxed));
        }
    }
}
