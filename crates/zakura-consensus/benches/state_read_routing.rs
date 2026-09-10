//! Unloaded latency benchmark for transaction-verifier state read routing.

#![allow(clippy::print_stdout)]

use std::{env, time::Duration};

const DEFAULT_REQUESTS_PER_SAMPLE: usize = 100_000;
const DEFAULT_SAMPLES: usize = 10;

fn main() {
    let requests = env_usize(
        "ZAKURA_STATE_READ_ROUTING_REQUESTS",
        DEFAULT_REQUESTS_PER_SAMPLE,
    );
    let samples = env_usize("ZAKURA_STATE_READ_ROUTING_SAMPLES", DEFAULT_SAMPLES);
    assert!(samples > 0, "the benchmark needs at least one sample");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the benchmark runtime builds");

    let mut buffered = Vec::with_capacity(samples);
    let mut direct = Vec::with_capacity(samples);
    for sample in 0..samples {
        let (buffered_elapsed, direct_elapsed) = runtime.block_on(
            zakura_consensus::router::benchmark_transaction_state_read_routing(
                requests,
                sample % 2 == 0,
            ),
        );
        buffered.push(buffered_elapsed);
        direct.push(direct_elapsed);
    }

    print_result("buffered_state_read", requests, buffered);
    print_result("direct_state_read", requests, direct);
}

fn print_result(operation: &str, requests: usize, mut timings: Vec<Duration>) {
    timings.sort_unstable();
    let median = timings[timings.len() / 2];
    let p95 = timings[timings.len().saturating_sub(1) * 95 / 100];
    let requests = u32::try_from(requests).expect("the benchmark request count fits in u32");
    let median_ns_per_request = median.as_secs_f64() * 1_000_000_000.0 / f64::from(requests);
    let p95_ns_per_request = p95.as_secs_f64() * 1_000_000_000.0 / f64::from(requests);

    println!(
        "operation={operation} samples={} requests_per_sample={requests} \
         median_ns_per_request={median_ns_per_request:.1} \
         p95_ns_per_request={p95_ns_per_request:.1}",
        timings.len(),
    );
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .map(|value| value.parse().expect("benchmark setting must be a usize"))
        .unwrap_or(default)
}
