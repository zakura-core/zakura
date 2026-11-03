//! Cost of boxing supervised peer tasks.
//!
//! Per-connection peer tasks and pipes are spawned boxed, so all callers share one
//! task type. Per-request serving tasks stay unboxed. This compares the two paths:
//! spawning and joining tasks, spawning a task with a large state machine, and
//! polling a task many times, where the boxed path adds an indirect call per poll.
#![allow(missing_docs)]

use std::{future::Future, hint::black_box};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio::{runtime::Runtime, task::JoinHandle};

use zakura_network::zakura::{supervised_spawn_bench, ZakuraPeerId};

/// Tasks spawned and joined per iteration, so runtime entry is amortized.
const TASKS_PER_ITER: u64 = 100;

/// Times the polling task yields back to the scheduler.
const YIELDS: u64 = 1_000;

/// Bytes a large task keeps alive across an await, like a peer pipe's state.
const LARGE_STATE_BYTES: usize = 4096;

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a current-thread runtime builds without I/O or timers")
}

fn peer_id() -> ZakuraPeerId {
    ZakuraPeerId::new(vec![1, 2, 3]).expect("3-byte id is within the node-id bound")
}

async fn empty() {}

async fn large_state() {
    let state = [7u8; LARGE_STATE_BYTES];
    tokio::task::yield_now().await;
    black_box(&state);
}

async fn yields() {
    for _ in 0..YIELDS {
        tokio::task::yield_now().await;
    }
}

/// The two spawn paths, named for the benchmark IDs.
fn paths() -> [(&'static str, bool); 2] {
    [("boxed", true), ("unboxed", false)]
}

/// Spawn `task` through the selected path; both paths receive the same future type.
fn spawn(boxed: bool, task: impl Future<Output = ()> + Send + 'static) -> JoinHandle<()> {
    if boxed {
        supervised_spawn_bench::spawn_boxed(peer_id(), task)
    } else {
        supervised_spawn_bench::spawn_unboxed(peer_id(), task)
    }
}

fn spawn_and_join(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("supervised_spawn/spawn_join");
    group.throughput(Throughput::Elements(TASKS_PER_ITER));

    for (name, boxed) in paths() {
        group.bench_function(BenchmarkId::new("empty", name), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let handles: Vec<_> =
                        (0..TASKS_PER_ITER).map(|_| spawn(boxed, empty())).collect();
                    for handle in handles {
                        handle.await.expect("benchmark task does not panic");
                    }
                })
            })
        });
        group.bench_function(BenchmarkId::new("large_state", name), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let handles: Vec<_> = (0..TASKS_PER_ITER)
                        .map(|_| spawn(boxed, large_state()))
                        .collect();
                    for handle in handles {
                        handle.await.expect("benchmark task does not panic");
                    }
                })
            })
        });
    }
    group.finish();
}

fn polls(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("supervised_spawn/polls");
    group.throughput(Throughput::Elements(YIELDS));

    for (name, boxed) in paths() {
        group.bench_function(BenchmarkId::new("yield", name), |b| {
            b.iter(|| {
                rt.block_on(async {
                    spawn(boxed, yields())
                        .await
                        .expect("benchmark task does not panic");
                })
            })
        });
    }
    group.finish();
}

criterion_group!(benches, spawn_and_join, polls);
criterion_main!(benches);
