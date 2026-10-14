//! Manual storage evidence for regulated GetBlocks serving reads.
//!
//! Serving runs at most one read-and-encode job per node execution slot,
//! however many peers are admitted. The concurrency sweep below measures that
//! job at and around the execution limit.

use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use zakura_chain::{block::Height, parameters::Network, serialization::ZcashSerialize};
use zakura_test::process_stats::{cpu_seconds, memory_kib};

use crate::{init_read_only, Config, PruningConfig, StorageMode};

/// Production advertises this many body bytes per response by default.
const MAX_RESPONSE_BYTES: u32 = 32 * 1024 * 1024;

/// What one worker records for one case.
#[derive(Default)]
struct Tally {
    reads: u64,
    blocks: u64,
    bytes: u64,
    latencies_us: Vec<u32>,
    encode_us: Vec<u32>,
}

/// Encode blocks the way the GetBlocks producer does: serialize each block,
/// then copy it behind its one-byte message tag. Returns the frame bytes.
fn encode_like_serving(blocks: &[(Height, Arc<zakura_chain::block::Block>, usize)]) -> u64 {
    blocks
        .iter()
        .map(|(_, block, _)| {
            let bytes = block
                .zcash_serialize_to_vec()
                .expect("stored blocks serialize");
            let mut payload = Vec::with_capacity(bytes.len() + 1);
            payload.push(3);
            payload.extend_from_slice(&bytes);
            u64::try_from(payload.len()).expect("frame length fits u64")
        })
        .sum()
}

/// Microseconds since `began`, saturating at `u32::MAX`.
fn elapsed_us(began: Instant) -> u32 {
    u32::try_from(began.elapsed().as_micros()).unwrap_or(u32::MAX)
}

/// Times production block-range reads against a synced database.
///
/// Set `ZAKURA_STORAGE_PROBE_CACHE_DIR` to a state cache directory and
/// optionally `ZAKURA_STORAGE_PROBE_NETWORK=mainnet`. The database opens in
/// pruned mode unless `ZAKURA_STORAGE_PROBE_ARCHIVE=1`. With
/// `ZAKURA_PROBE_DROP_CACHES=1`, running as root, each case starts with a cold
/// page cache. With `ZAKURA_STORAGE_PROBE_ENCODE=1`, each read is followed by
/// the serving encode on a blocking thread, and `p50_us`/`p99_us` cover both.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "manual storage evidence; requires ZAKURA_STORAGE_PROBE_CACHE_DIR"]
#[allow(clippy::print_stderr)]
async fn getblocks_storage_read_probe() {
    let Some(cache_dir) = std::env::var_os("ZAKURA_STORAGE_PROBE_CACHE_DIR") else {
        return;
    };
    let network = match std::env::var("ZAKURA_STORAGE_PROBE_NETWORK").as_deref() {
        Ok("mainnet") => Network::Mainnet,
        _ => Network::new_default_testnet(),
    };
    let seconds = std::env::var("ZAKURA_PROBE_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10);
    let drop_caches = std::env::var_os("ZAKURA_PROBE_DROP_CACHES").is_some();
    let encode = std::env::var_os("ZAKURA_STORAGE_PROBE_ENCODE").is_some();
    let storage_mode = if std::env::var_os("ZAKURA_STORAGE_PROBE_ARCHIVE").is_some() {
        StorageMode::Archive
    } else {
        StorageMode::Pruned(PruningConfig::default())
    };
    let config = Config {
        cache_dir: cache_dir.into(),
        storage_mode,
        ..Config::default()
    };
    let (state, db, _non_finalized) = init_read_only(config, &network).unwrap();
    let tip = db
        .finalized_tip_height()
        .expect("the probe needs a synced database");

    // Pruned databases keep a contiguous suffix of bodies; find where it starts.
    let has_body = |height: u32| {
        let mut state = state.clone();
        async move {
            let range = state
                .read_owned_block_range(Height(height), 1, MAX_RESPONSE_BYTES, (), |_| false)
                .await
                .unwrap();
            !range.into_parts().0.is_empty()
        }
    };
    let (mut low, mut high) = (0, tip.0);
    while low < high {
        let middle = low + (high - low) / 2;
        if has_body(middle).await {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    let retained = (low, tip.0);
    eprintln!(
        "storage_probe network={network} tip={} retained_low={low} rss_kib={:?}",
        tip.0,
        memory_kib()
    );

    for (shape, count) in [
        ("sequential_near_tip", 1_u32),
        ("random_retained", 1),
        ("random_retained_16", 16),
        ("unavailable_above_tip", 1),
    ] {
        for concurrency in [1_usize, 4, 8, 16, 19, 24, 32, 38, 48, 64, 76] {
            if drop_caches {
                std::process::Command::new("sync").status().unwrap();
                std::fs::write("/proc/sys/vm/drop_caches", "3").unwrap();
            }
            let next = Arc::new(AtomicU32::new(
                tip.0.saturating_sub(200_000).max(retained.0),
            ));
            let cpu_start = cpu_seconds();
            let deadline = Instant::now() + Duration::from_secs(seconds);
            let workers: Vec<_> = (0..concurrency)
                .map(|worker| {
                    let mut state = state.clone();
                    let next = next.clone();
                    // A small xorshift keeps random heights reproducible per worker.
                    let mut seed = 0x9E37_79B9_u32 ^ u32::try_from(worker).unwrap();
                    tokio::spawn(async move {
                        let mut tally = Tally::default();
                        while Instant::now() < deadline {
                            seed ^= seed << 13;
                            seed ^= seed >> 17;
                            seed ^= seed << 5;
                            let span = retained.1 - retained.0 + 1;
                            let start = match shape {
                                "sequential_near_tip" => {
                                    let height = next.fetch_add(count, Ordering::Relaxed);
                                    if height > retained.1 {
                                        next.store(retained.0, Ordering::Relaxed);
                                    }
                                    height.min(retained.1)
                                }
                                "unavailable_above_tip" => retained.1 + 1 + seed % 1_000,
                                _ => retained.0 + seed % span,
                            };
                            let began = Instant::now();
                            let range = state
                                .read_owned_block_range(
                                    Height(start),
                                    count,
                                    MAX_RESPONSE_BYTES,
                                    (),
                                    |_| false,
                                )
                                .await
                                .unwrap();
                            let (blocks, ()) = range.into_parts();
                            tally.blocks += u64::try_from(blocks.len()).unwrap();
                            tally.bytes += if encode {
                                let encoding = Instant::now();
                                let bytes = tokio::task::spawn_blocking(move || {
                                    encode_like_serving(&blocks)
                                })
                                .await
                                .unwrap();
                                tally.encode_us.push(elapsed_us(encoding));
                                bytes
                            } else {
                                blocks
                                    .iter()
                                    .map(|(_, _, size)| u64::try_from(*size).unwrap())
                                    .sum::<u64>()
                            };
                            tally.latencies_us.push(elapsed_us(began));
                            tally.reads += 1;
                        }
                        tally
                    })
                })
                .collect();
            let mut total = Tally::default();
            for worker in workers {
                let tally = worker.await.unwrap();
                total.reads += tally.reads;
                total.blocks += tally.blocks;
                total.bytes += tally.bytes;
                total.latencies_us.extend(tally.latencies_us);
                total.encode_us.extend(tally.encode_us);
            }
            let cpu = cpu_seconds().zip(cpu_start).map(|(end, start)| end - start);
            total.latencies_us.sort_unstable();
            total.encode_us.sort_unstable();
            let at = |values: &[u32], p: usize| {
                values
                    .get((values.len().saturating_sub(1)) * p / 100)
                    .copied()
                    .unwrap_or(0)
            };
            let percentile = |p: usize| at(&total.latencies_us, p);
            // Float conversions are lossy only far beyond these counts.
            let per_second = |value: u64| value as f64 / seconds as f64;
            eprintln!(
                "storage_probe shape={shape} count={count} concurrency={concurrency} \
                 cold_cache={drop_caches} encode={encode} reads_per_s={:.0} blocks_per_s={:.0} \
                 mb_per_s={:.2} p50_us={} p99_us={} max_us={} encode_p50_us={} encode_p99_us={} \
                 cpu_s={cpu:?} rss_kib={:?}",
                per_second(total.reads),
                per_second(total.blocks),
                per_second(total.bytes) / 1e6,
                percentile(50),
                percentile(99),
                total.latencies_us.last().copied().unwrap_or(0),
                at(&total.encode_us, 50),
                at(&total.encode_us, 99),
                memory_kib(),
            );
        }
    }
}
