//! Concurrent queue ownership and capacity notifications; no transport.
#![allow(missing_docs)]

use std::{
    num::NonZeroU64,
    sync::Barrier,
    thread,
    time::{Duration, Instant},
};

use zakura_network::zakura::BenchWorkChurn;

#[allow(
    clippy::print_stdout,
    reason = "this benchmark emits JSON measurements"
)]
fn main() {
    let smoke = std::env::args().any(|arg| arg == "--test");
    let iterations = if smoke { 50 } else { 5_000 };
    for peers in [1, 4, 16] {
        for saturated in [false, true] {
            let slots = if saturated { 1 } else { peers };
            let work = BenchWorkChurn::new(peers, 16, 4096, slots);
            let barrier = Barrier::new(usize::try_from(peers).unwrap() + 1);
            let (elapsed, results) = thread::scope(|scope| {
                let handles: Vec<_> = (0..peers)
                    .map(|peer| {
                        let mut work = work.clone();
                        let barrier = &barrier;
                        scope.spawn(move || {
                            let runtime = tokio::runtime::Builder::new_current_thread()
                                .enable_time()
                                .build()
                                .unwrap();
                            let capacity = work.capacity_reader();
                            let mut latencies = Vec::with_capacity(iterations);
                            let mut refusals = 0u64;
                            let mut wakeups = 0u64;
                            let mut attempt = 0u64;
                            barrier.wait();
                            let start = Instant::now();
                            let deadline = start + Duration::from_secs(30);
                            runtime.block_on(async {
                                for _ in 0..iterations {
                                    let cycle = Instant::now();
                                    loop {
                                        let notified = capacity.subscribe().notified();
                                        tokio::pin!(notified);
                                        notified.as_mut().enable();
                                        attempt += 1;
                                        if work.cycle(
                                            peer,
                                            NonZeroU64::new(attempt).unwrap(),
                                            || {
                                                if saturated {
                                                    thread::yield_now();
                                                }
                                            },
                                        ) {
                                            break;
                                        }
                                        refusals += 1;
                                        assert!(
                                            Instant::now() < deadline,
                                            "peer {peer} stopped making progress"
                                        );
                                        if capacity.available() < 16 * 4096 {
                                            tokio::time::timeout_at(
                                                tokio::time::Instant::from_std(deadline),
                                                notified,
                                            )
                                            .await
                                            .expect("capacity release must wake the waiting peer");
                                            wakeups += 1;
                                        }
                                    }
                                    latencies
                                        .push(u64::try_from(cycle.elapsed().as_nanos()).unwrap());
                                }
                            });
                            (peer, refusals, wakeups, start.elapsed(), latencies)
                        })
                    })
                    .collect();
                let start = Instant::now();
                barrier.wait();
                let results: Vec<_> = handles
                    .into_iter()
                    .map(|handle| handle.join().unwrap())
                    .collect();
                (start.elapsed(), results)
            });
            work.assert_settled();
            let results: Vec<_> = results
                .into_iter()
                .map(|(peer, refusals, wakeups, elapsed, mut latencies)| {
                    latencies.sort_unstable();
                    serde_json::json!({
                        "peer": peer, "completed": latencies.len(), "refusals": refusals,
                        "capacity_wakeups": wakeups,
                        "elapsed_ns": u64::try_from(elapsed.as_nanos()).unwrap(),
                        "p50_ns": latencies[latencies.len() / 2],
                        "p99_ns": latencies[(latencies.len() - 1) * 99 / 100],
                        "max_ns": latencies.last().unwrap(),
                    })
                })
                .collect();
            println!(
                "{}",
                serde_json::json!({
                    "peers": peers, "active_slots": slots, "hold_yield": saturated,
                    "iterations": iterations, "items_per_request": 16, "estimated_bytes": 4096,
                "floor_requested": false, "retry": "capacity_notification",
                    "elapsed_ns": u64::try_from(elapsed.as_nanos()).unwrap(), "results": results,
                })
            );
        }
    }
}
