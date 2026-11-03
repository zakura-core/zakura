//! Checks and measurements for rejecting chain-ineligible mempool transactions.

use super::*;

/// Obtain the real unknown-anchor error without exposing constructors for state errors.
async fn unknown_anchor_error(tx: &Transaction) -> ValidateContextError {
    let state = zakura_state::init_test(&Network::Mainnet).await;
    let error = state
        .oneshot(zakura_state::Request::CheckBestChainTipNullifiersAndAnchors(tx.clone().into()))
        .await
        .expect_err("an empty chain cannot recognize the fixture's spend anchor");
    *error
        .downcast::<ValidateContextError>()
        .expect("the real state service returns a context error")
}

/// Count submissions for one bundle, isolating the measurement from other transactions.
fn proof_calls(tx: &Transaction, upgrade: NetworkUpgrade) -> usize {
    if tx.has_sapling_shielded_data() {
        primitives::sapling::inner_calls_for(&sapling_item(tx, upgrade))
    } else {
        primitives::halo2::inner_calls_for(upgrade, &orchard_item(tx, upgrade))
    }
}

/// Run alone in a release test executable on Linux, with four Tokio and Rayon threads.
///
/// Repeated valid fixtures model cold verification bursts, not unique network admissions.
/// Actual proof submissions are reported because later requests can hit the bundle cache.
#[cfg(all(feature = "internal-bench", target_os = "linux"))]
#[test]
#[ignore = "requires an isolated host; run this test alone in release mode"]
#[allow(clippy::print_stdout)]
fn rejected_anchor_verification_load() {
    use futures::{stream::FuturesUnordered, StreamExt};
    use std::time::Instant;

    let _init_guard = zakura_test::init();
    rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build_global()
        .expect("run the benchmark alone so it owns Rayon's global pool");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("the benchmark runtime can be created");

    runtime.block_on(async {
        let orchard = cacheable_mainnet_orchard_transaction();
        let orchard_height = orchard.expiry_height().expect("the fixture has an expiry height");
        let orchard_upgrade = NetworkUpgrade::current(&Network::Mainnet, orchard_height);
        let fixtures = [cacheable_mainnet_sapling_transaction(), (orchard_upgrade, orchard)];

        for (upgrade, rejected) in fixtures {
            let error = unknown_anchor_error(&rejected).await;
            let (probe_height, probe) = test_transactions(&Network::Mainnet)
                .find(|(height, tx)| {
                    tx.inputs().is_empty()
                        && tx.hash() != rejected.hash()
                        && NetworkUpgrade::current(&Network::Mainnet, *height) == upgrade
                        && if rejected.has_sapling_shielded_data() {
                            tx.has_sapling_shielded_data() && tx.orchard_shielded_data().is_none()
                        } else {
                            tx.orchard_shielded_data().is_some() && !tx.has_sapling_shielded_data()
                        }
                })
                .expect("the vectors contain a separate shielded-only block probe");
            let state = service_fn(move |request| {
                assert!(matches!(request, zakura_state::Request::CheckBestChainTipNullifiersAndAnchors(_)));
                let error: BoxError = Box::new(error.clone());
                async move { Err::<zakura_state::Response, _>(error) }
            });
            let (setup_tx, setup_rx) = tokio::sync::oneshot::channel();
            let mut verifier = Verifier::new(&Network::Mainnet, state, setup_rx);
            setup_tx.send(CacheTestMempool::build().for_prop_tests()).ok().unwrap();
            let height = rejected.expiry_height().expect("the fixture has an expiry height");

            // Warm the circuit keys and runtime before measuring. Each trial then starts cold.
            verifier.ready().await.unwrap().call(block_request(&rejected, height)).await.unwrap();
            for burst in [0, 5, 500] {
                for trial in 0..=1 {
                    primitives::clear_shielded_verification_caches();
                    let before = proof_calls(&rejected, upgrade);
                    let cpu_before = process_cpu_ticks();
                    let started = Instant::now();
                    let mut pending = FuturesUnordered::new();
                    for _ in 0..burst {
                        let future = verifier.ready().await.unwrap().call(Request::Mempool {
                            transaction: rejected.clone().into(),
                            height,
                        });
                        pending.push(tokio::spawn(future));
                    }
                    // Give queued mempool requests a chance to enter the shared verifiers.
                    tokio::task::yield_now().await;
                    let probe_started = Instant::now();
                    verifier.ready().await.unwrap().call(block_request(&probe, probe_height)).await.unwrap();
                    let probe_seconds = probe_started.elapsed().as_secs_f64();
                    while let Some(result) = pending.next().await {
                        assert!(matches!(result.unwrap(), Err(TransactionError::ValidateContextError(_))));
                    }
                    let elapsed = started.elapsed().as_secs_f64();
                    let submissions = proof_calls(&rejected, upgrade) - before;
                    if std::env::var_os("ZAKURA_ANCHOR_BENCH_EXPECT_NO_PROOFS").is_some() {
                        assert_eq!(submissions, 0, "rejected requests must not reach proof verification");
                    }
                    println!("anchor_bench pool={} burst={burst} warmup={} seconds={elapsed:.6} probe_seconds={probe_seconds:.6} cpu_ticks={} proof_submissions={submissions}",
                        if rejected.has_sapling_shielded_data() { "sapling" } else { "orchard" },
                        trial == 0, process_cpu_ticks() - cpu_before);
                }
            }
        }
    });
}

/// Linux reports user and system CPU consumption in clock ticks, including worker threads.
#[cfg(all(feature = "internal-bench", target_os = "linux"))]
fn process_cpu_ticks() -> u64 {
    let stat = std::fs::read_to_string("/proc/self/stat").expect("Linux exposes process CPU usage");
    let fields: Vec<_> = stat
        .rsplit_once(") ")
        .expect("the process name is delimited")
        .1
        .split_whitespace()
        .collect();
    fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap()
}
