//! Manual state-progress probe for blocks packed to the serialized size limit.

use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use futures::{FutureExt, StreamExt};
use tower::{buffer::Buffer, Service, ServiceExt};
use zakura_chain::{
    block::{Block, MAX_BLOCK_BYTES},
    parameters::NetworkUpgrade,
    serialization::{CompactSizeMessage, ZcashDeserializeInto, ZcashSerialize},
    transaction::LockTime,
};

use super::*;
use crate::{service::StateService, Config, ReadRequest, ReadResponse, Request, Response};

fn outpoint(index: u32) -> transparent::OutPoint {
    let mut hash = [0; 32];
    hash[..4].copy_from_slice(&index.to_le_bytes());
    transparent::OutPoint {
        hash: hash.into(),
        index: 0,
    }
}

fn lookup_transaction(first_input: u32, input_count: u32) -> Transaction {
    Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu5,
        inputs: (first_input..first_input + input_count)
            .map(|index| Input::PrevOut {
                outpoint: outpoint(index),
                unlock_script: transparent::Script::new(&[]),
                sequence: u32::MAX,
            })
            .collect(),
        outputs: vec![transparent::Output {
            value: Amount::<NonNegative>::try_from(u64::from(input_count) * 1000).unwrap(),
            lock_script: transparent::Script::new(&[]),
        }],
        lock_time: LockTime::unlocked(),
        expiry_height: Height(1_687_126),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    }
}

fn full_lookup_block(inputs_per_transaction: u32) -> Block {
    let mut block: Block = zakura_test::vectors::BLOCK_MAINNET_1687106_BYTES
        .zcash_deserialize_into()
        .unwrap();
    block.transactions.truncate(1);
    let mut bytes =
        block.header.zcash_serialized_size() + block.transactions[0].zcash_serialized_size();
    let limit = usize::try_from(MAX_BLOCK_BYTES).unwrap();
    let mut first_input = 0;
    loop {
        let count_bytes = CompactSizeMessage::try_from(block.transactions.len() + 1)
            .unwrap()
            .zcash_serialized_size();
        let mut input_count = inputs_per_transaction;
        let next = loop {
            let tx = lookup_transaction(first_input, input_count);
            if bytes + count_bytes + tx.zcash_serialized_size() <= limit {
                break Some(tx);
            }
            input_count -= 1;
            if input_count == 0 {
                break None;
            }
        };
        let Some(tx) = next else { break };
        bytes += tx.zcash_serialized_size();
        first_input += input_count;
        block.transactions.push(Arc::new(tx));
    }
    let size = block.zcash_serialized_size();
    assert!(size <= limit);
    // No further one-input transaction fits after accounting for the count prefix.
    assert!(limit - size < lookup_transaction(first_input, 1).zcash_serialized_size());
    block
}

// The guard releases reserved workers if the probe panics or its timeout expires.
struct BlockingPressure(Arc<AtomicBool>);

impl Drop for BlockingPressure {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

async fn reserve_workers(count: usize) -> BlockingPressure {
    let pressure = BlockingPressure(Arc::new(AtomicBool::new(false)));
    for _ in 0..count {
        let stop = pressure.0.clone();
        let (ready, started) = tokio::sync::oneshot::channel();
        tokio::task::spawn_blocking(move || {
            ready.send(()).unwrap();
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        started.await.unwrap();
    }
    pressure
}

#[test]
#[ignore = "manual database contention probe; run with --ignored --nocapture"]
fn utxo_state_contention() {
    let _init_guard = zakura_test::init();
    // The constrained case leaves only four workers for all state reads and probes.
    for (blocking_threads, reserved) in [(512, 0), (32, 28)] {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .max_blocking_threads(blocking_threads)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(180), async {
                for inputs_per_transaction in [64, 1001] {
                    measure(blocking_threads, reserved, inputs_per_transaction).await;
                }
            })
            .await
            .unwrap();
        });
    }
}

#[allow(clippy::print_stdout)]
async fn measure(blocking_threads: usize, reserved: usize, inputs_per_transaction: u32) {
    // Sixty-four inputs maximize concurrent requests per byte in this transaction shape.
    // The other case packs high-input transactions into the same two-megabyte envelope.
    let block = full_lookup_block(inputs_per_transaction);
    let inputs: Vec<Vec<_>> = block.transactions[1..]
        .iter()
        .map(|tx| {
            tx.inputs()
                .iter()
                .map(|input| input.outpoint().unwrap())
                .collect()
        })
        .collect();
    let outpoints: Vec<_> = inputs.iter().flatten().copied().collect();
    let peak: usize = inputs.iter().map(|inputs| inputs.len().min(64)).sum();
    println!(
        "shape={inputs_per_transaction} bytes={} tx={} inputs={} max_pending={peak}",
        block.zcash_serialized_size(),
        inputs.len(),
        outpoints.len()
    );

    for (mode, width, admit_one_at_a_time) in [
        ("serial", 1, true),
        ("queued-64", 64, false),
        ("admitted-64", 64, true),
    ] {
        // The probe creates a fresh database for each mode, including its first SST read.
        let (state, _, _, _) =
            StateService::new(Config::ephemeral(), &Network::Mainnet, Height::MAX, 0)
                .await
                .unwrap();
        let read_state = state.read_service.clone();
        let probe_outpoint = outpoint(u32::MAX);
        let db = &state.read_service.db.db;
        let locations = db.cf_handle("tx_loc_by_hash").unwrap();
        let outputs = db.cf_handle("utxo_by_out_loc").unwrap();
        let mut write = DiskWriteBatch::new();
        // These indexes serve the serialized block's disjoint inputs, not a full chain.
        for (index, outpoint) in outpoints.iter().chain([&probe_outpoint]).enumerate() {
            let location = TransactionLocation::from_usize(Height(1), index + 1);
            write.zs_insert(&locations, outpoint.hash, location);
            write.zs_insert(
                &outputs,
                OutputLocation::from_outpoint(location, outpoint),
                transparent::Output {
                    value: Amount::<NonNegative>::try_from(1000u64).unwrap(),
                    lock_script: transparent::Script::new(&[0x51]),
                },
            );
        }
        db.write(write).unwrap();
        db.flush_cf(&locations).unwrap();
        db.flush_cf(&outputs).unwrap();
        let state = Buffer::new(state, 64);
        let _pressure = reserve_workers(reserved).await;

        for (phase, rounds) in [("first-SST-read", 1), ("warm", 10)] {
            let done = Arc::new(AtomicBool::new(false));
            let progress = Arc::new(AtomicUsize::new(0));
            let probe_done = done.clone();
            let probe_progress = progress.clone();
            let probe_read = read_state.clone();
            let probe_state = state.clone();
            let probe = tokio::spawn(async move {
                let mut reads = Vec::new();
                let mut tips = Vec::new();
                let mut queues = Vec::new();
                while !probe_done.load(Ordering::Relaxed) {
                    let start = Instant::now();
                    let response = tokio::time::timeout(
                        Duration::from_secs(1),
                        probe_read
                            .clone()
                            .oneshot(ReadRequest::UnspentBestChainUtxo(probe_outpoint)),
                    )
                    .await
                    .expect("unrelated read must complete within one second")
                    .unwrap();
                    assert!(matches!(
                        response,
                        ReadResponse::UnspentBestChainUtxo(Some(_))
                    ));
                    reads.push(start.elapsed());
                    let start = Instant::now();
                    let response = tokio::time::timeout(
                        Duration::from_secs(1),
                        probe_state.clone().oneshot(Request::Tip),
                    )
                    .await
                    .expect("buffered state request must complete within one second")
                    .unwrap();
                    assert!(matches!(response, Response::Tip(_)));
                    tips.push(start.elapsed());
                    let start = Instant::now();
                    queues.push(
                        tokio::time::timeout(
                            Duration::from_secs(1),
                            tokio::task::spawn_blocking(move || start.elapsed()),
                        )
                        .await
                        .expect("blocking probe must start within one second")
                        .unwrap(),
                    );
                    probe_progress.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                (reads, tips, queues)
            });
            let start = Instant::now();
            let mut min_progress = usize::MAX;
            for _ in 0..rounds {
                let before = progress.load(Ordering::Relaxed);
                futures::stream::iter(&inputs)
                    .for_each_concurrent(inputs.len(), |inputs| {
                        let state = state.clone();
                        async move {
                            let admissions =
                                futures::stream::iter(inputs.iter().copied()).map(|outpoint| {
                                    let mut state = state.clone();
                                    async move {
                                        state
                                            .ready()
                                            .await
                                            .map(|state| state.call(Request::AwaitUtxo(outpoint)))
                                    }
                                });
                            let reads = if admit_one_at_a_time {
                                admissions
                                    .then(std::convert::identity)
                                    .map(|response| response.unwrap().left_future())
                                    .left_stream()
                            } else {
                                admissions
                                    .map(|admit| async move { admit.await.unwrap().await })
                                    .map(FutureExt::right_future)
                                    .right_stream()
                            };
                            let reads = reads.buffer_unordered(width);
                            futures::pin_mut!(reads);
                            let mut count = 0;
                            while let Some(response) = reads.next().await {
                                assert!(matches!(response.unwrap(), Response::Utxo(_)));
                                count += 1;
                            }
                            assert_eq!(count, inputs.len());
                        }
                    })
                    .await;
                let completed = progress.load(Ordering::Relaxed) - before;
                assert!(
                    completed > 0,
                    "unrelated state work must progress during every block"
                );
                min_progress = min_progress.min(completed);
            }
            let elapsed = start.elapsed();
            done.store(true, Ordering::Relaxed);
            let (mut reads, mut tips, mut queues) = probe.await.unwrap();
            reads.sort_unstable();
            tips.sort_unstable();
            queues.sort_unstable();
            println!("pool={blocking_threads} reserved={reserved} shape={inputs_per_transaction} mode={mode} width={width} phase={phase} block_ms={:.3} samples={} min_progress={min_progress} read_p99_us={} read_max_us={} tip_p99_us={} tip_max_us={}",
                elapsed.as_secs_f64() * 1000.0 / f64::from(rounds), reads.len(),
                reads[(reads.len()-1)*99/100].as_micros(), reads.last().unwrap().as_micros(),
                tips[(tips.len()-1)*99/100].as_micros(), tips.last().unwrap().as_micros());
            println!(
                "queue_p99_us={} queue_max_us={}",
                queues[(queues.len() - 1) * 99 / 100].as_micros(),
                queues.last().unwrap().as_micros()
            );
        }
    }
}
