//! Manual contention probe for the existing single-output state path.

use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use futures::StreamExt;
use tower::{buffer::Buffer, ServiceExt};

use super::*;
use crate::{service::StateService, Config, ReadRequest, ReadResponse, Request, Response};

#[test]
#[ignore = "manual database contention probe; run with --ignored --nocapture"]
fn utxo_state_contention() {
    let _init_guard = zakura_test::init();
    for blocking_threads in [32, 512] {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .max_blocking_threads(blocking_threads)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(120), measure(blocking_threads))
                .await
                .unwrap();
        });
    }
}

#[allow(clippy::print_stdout)]
async fn measure(blocking_threads: usize) {
    let (state, _, _, _) =
        StateService::new(Config::ephemeral(), &Network::Mainnet, Height::MAX, 0)
            .await
            .unwrap();
    let read_state = state.read_service.clone();
    let mut outpoints = Vec::new();
    let db = &state.read_service.db.db;
    let locations = db.cf_handle("tx_loc_by_hash").unwrap();
    let outputs = db.cf_handle("utxo_by_out_loc").unwrap();
    let mut write = DiskWriteBatch::new();
    // Disjoint input sets model 16 transactions with 1,001 external inputs each.
    // These indexes exercise real reads but do not form a valid chain.
    for index in 0u32..16 * 1001 {
        let mut hash = [0; 32];
        hash[..4].copy_from_slice(&index.to_le_bytes());
        let outpoint = transparent::OutPoint {
            hash: hash.into(),
            index: 0,
        };
        let location =
            TransactionLocation::from_usize(Height(1), usize::try_from(index).unwrap() + 1);
        write.zs_insert(&locations, outpoint.hash, location);
        write.zs_insert(
            &outputs,
            OutputLocation::from_outpoint(location, &outpoint),
            transparent::Output {
                value: Amount::<NonNegative>::try_from(u64::from(index) + 1).unwrap(),
                lock_script: transparent::Script::new(&[0x51]),
            },
        );
        outpoints.push(outpoint);
    }
    db.write(write).unwrap();
    db.flush_cf(&locations).unwrap();
    db.flush_cf(&outputs).unwrap();
    let state = Buffer::new(state, 64);
    for outpoint in &outpoints {
        state
            .clone()
            .oneshot(Request::AwaitUtxo(*outpoint))
            .await
            .unwrap();
    }

    for repeat in 0..2 {
        let widths = if repeat == 0 { [1, 64] } else { [64, 1] };
        for transactions in [0, 4, 16] {
            for width in widths {
                if transactions == 0 && width == 64 {
                    continue;
                }
                let done = Arc::new(AtomicBool::new(false));
                let probe_done = done.clone();
                let probe_state = read_state.clone();
                let probe = tokio::spawn(async move {
                    let mut reads = Vec::new();
                    let mut queues = Vec::new();
                    while !probe_done.load(Ordering::Relaxed) {
                        let start = Instant::now();
                        // A missing transaction exercises an RPC-facing database read.
                        let response = probe_state
                            .clone()
                            .oneshot(ReadRequest::Transaction([0xff; 32].into()))
                            .await
                            .unwrap();
                        assert!(matches!(response, ReadResponse::Transaction(None)));
                        reads.push(start.elapsed());
                        let start = Instant::now();
                        queues.push(
                            tokio::task::spawn_blocking(move || start.elapsed())
                                .await
                                .unwrap(),
                        );
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    (reads, queues)
                });
                let start = Instant::now();
                let rounds = 20;
                if transactions == 0 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                } else {
                    for _ in 0..rounds {
                        futures::stream::iter(outpoints[..transactions * 1001].chunks(1001))
                            .for_each_concurrent(transactions, |inputs| {
                                let state = state.clone();
                                async move {
                                    let mut reads = futures::stream::iter(inputs.iter().copied())
                                        .map(|outpoint| {
                                            state.clone().oneshot(Request::AwaitUtxo(outpoint))
                                        })
                                        .buffer_unordered(width);
                                    let mut count = 0;
                                    while let Some(response) = reads.next().await {
                                        assert!(matches!(response.unwrap(), Response::Utxo(_)));
                                        count += 1;
                                    }
                                    assert_eq!(count, 1001);
                                }
                            })
                            .await;
                    }
                }
                let elapsed = start.elapsed();
                done.store(true, Ordering::Relaxed);
                let (mut reads, mut queues) = probe.await.unwrap();
                reads.sort_unstable();
                queues.sort_unstable();
                assert!(!reads.is_empty());
                println!("pool={blocking_threads} repeat={repeat} tx={transactions} width={width} set_ms={:.3} samples={} read_p50_us={} read_p99_us={} read_max_us={} queue_p99_us={} queue_max_us={}",
                    elapsed.as_secs_f64() * 1000.0 / f64::from(rounds), reads.len(),
                    reads[reads.len()/2].as_micros(), reads[(reads.len()-1)*99/100].as_micros(), reads.last().unwrap().as_micros(),
                    queues[(queues.len()-1)*99/100].as_micros(), queues.last().unwrap().as_micros());
            }
        }
    }
}
