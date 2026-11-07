//! Checks and measurements for rejecting chain-ineligible mempool transactions.

use super::*;

#[tokio::test]
async fn pre_v4_mempool_transaction_is_rejected_without_state_requests() {
    let _init_guard = zakura_test::init();
    let mut joinsplit_data = test_transactions(&Network::Mainnet)
        .find_map(|(_, tx)| match tx.as_ref() {
            Transaction::V2 { joinsplit_data, .. } => joinsplit_data.clone(),
            _ => None,
        })
        .expect("mainnet vectors include a v2 transaction with a JoinSplit");
    joinsplit_data.rest.clear();
    joinsplit_data.first.vpub_old = 0.try_into().unwrap();
    joinsplit_data.first.vpub_new = 10_000.try_into().unwrap();
    joinsplit_data.first.anchor = [0x42; 32].into();
    let transaction = Transaction::V2 {
        inputs: Vec::new(),
        outputs: Vec::new(),
        lock_time: LockTime::unlocked(),
        joinsplit_data: Some(joinsplit_data),
    };
    // The JoinSplit supplies inputs, outputs, and a sufficient declared fee;
    // only its unsupported version should decide rejection before state access.
    check::has_inputs_and_outputs(&transaction).unwrap();
    let transaction: zakura_chain::transaction::UnminedTx = transaction.into();
    let fee = 10_000.try_into().unwrap();
    zip317::mempool_checks(
        zip317::unpaid_actions(&transaction, fee),
        fee,
        transaction.size(),
    )
    .unwrap();

    let mut state: MockService<zakura_state::Request, zakura_state::Response, PanicAssertion> =
        MockService::build().for_unit_tests();
    let verifier = Verifier::new_for_tests(&Network::Mainnet, state.clone());
    let verification = verifier.oneshot(Request::Mempool {
        transaction,
        height: NetworkUpgrade::Canopy
            .activation_height(&Network::Mainnet)
            .expect("Canopy has a mainnet activation height"),
    });
    tokio::pin!(verification);
    let result = futures::poll!(&mut verification);
    state.expect_no_requests().await;
    assert!(
        matches!(
            result,
            std::task::Poll::Ready(Err(TransactionError::WrongVersion))
        ),
        "unsupported versions must fail immediately with the structural version error"
    );
}

/// Blocks must use their own chain context, without the best-tip mempool gate.
pub(super) async fn check_block_skips_chain_gate(tx: &Transaction, height: Height) {
    // These shielded-only fixtures need no UTXO lookups. Reject every state
    // request, naming the mempool gate explicitly so a regression fails here.
    let state = service_fn(|request| async move {
        assert!(
            !matches!(
                request,
                zakura_state::Request::CheckBestChainTipNullifiersAndAnchors(_)
            ),
            "block verification must not query the best-tip mempool gate"
        );
        Err::<zakura_state::Response, BoxError>(
            "unexpected state request for shielded-only block transaction".into(),
        )
    });
    let verifier = Verifier::new_for_tests(&Network::Mainnet, state);
    timeout(test_timeout(), verifier.oneshot(block_request(tx, height)))
        .await
        .expect("block verification must finish without a state response")
        .expect("a valid block transaction must bypass the mempool chain gate");
}

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

/// Rejections and a pending lookup must not consume the fixture's cold proof verification.
pub(super) async fn check_chain_gate(
    verifier: &mut CacheTestVerifier,
    state: &CacheTestState,
    tx: &Transaction,
    upgrade: NetworkUpgrade,
) {
    use zakura_state::DuplicateNullifierError;

    let height = tx
        .expiry_height()
        .expect("the fixture has an expiry height");
    let anchor_error = unknown_anchor_error(tx).await;
    assert!(matches!(
        anchor_error,
        ValidateContextError::UnknownSaplingAnchor { .. }
            | ValidateContextError::UnknownOrchardAnchor { .. }
    ));
    let duplicate_error = if tx.has_sapling_shielded_data() {
        tx.sapling_nullifiers()
            .next()
            .unwrap()
            .duplicate_nullifier_error(true)
    } else {
        tx.orchard_nullifiers()
            .next()
            .unwrap()
            .duplicate_nullifier_error(true)
    };
    let expected_calls = proof_calls(tx, upgrade);
    let errors: [(BoxError, TransactionError); 3] = [
        (anchor_error.clone().into(), anchor_error.into()),
        (duplicate_error.clone().into(), duplicate_error.into()),
        (
            "state lookup unavailable".into(),
            TransactionError::from(BoxError::from("state lookup unavailable")),
        ),
    ];
    for (error, expected) in errors {
        let mut state = state.clone();
        let request = Request::Mempool {
            transaction: tx.clone().into(),
            height,
        };
        let verification = verify(verifier, request);
        tokio::pin!(verification);
        assert!(futures::poll!(&mut verification).is_pending());
        let response = state
            .expect_request_that(|req| {
                matches!(
                    req,
                    zakura_state::Request::CheckBestChainTipNullifiersAndAnchors(_)
                )
            })
            .await
            .unwrap();
        assert_eq!(
            proof_calls(tx, upgrade),
            expected_calls,
            "pending chain checks must not dispatch proofs"
        );
        response.respond_error(error);
        assert_eq!(verification.await.unwrap_err(), expected);
        assert_eq!(
            proof_calls(tx, upgrade),
            expected_calls,
            "rejected chain checks must not dispatch proofs"
        );
    }

    // Only a successful lookup may release the expensive verification.
    let mut state = state.clone();
    let verification = verify(
        verifier,
        Request::Mempool {
            transaction: tx.clone().into(),
            height,
        },
    );
    tokio::pin!(verification);
    assert!(futures::poll!(&mut verification).is_pending());
    let response = state
        .expect_request_that(|req| {
            matches!(
                req,
                zakura_state::Request::CheckBestChainTipNullifiersAndAnchors(_)
            )
        })
        .await
        .unwrap();
    assert_eq!(proof_calls(tx, upgrade), expected_calls);
    response.respond(zakura_state::Response::ValidBestChainTipNullifiersAndAnchors);
    verification
        .await
        .expect("valid chain context permits the real transaction to verify");
}

#[test]
fn state_timeout_does_not_dispatch_proofs() {
    let _init_guard = zakura_test::init();
    let mut tx = cacheable_mainnet_orchard_transaction();
    // The cache test uses expiry + 1 as a corrupted twin. Use a different key
    // so concurrent tests cannot change this request's submission counter.
    let height = tx.expiry_height().unwrap();
    *tx.expiry_height_mut() = (height + 2).unwrap();
    let upgrade = NetworkUpgrade::current(&Network::Mainnet, height);
    // The process-wide verifier's worker must belong to the persistent runtime,
    // rather than the short-lived runtime whose clock this test advances.
    let before = zakura_test::MULTI_THREADED_RUNTIME.block_on(async { proof_calls(&tx, upgrade) });
    assert_eq!(before, 0, "the timeout fixture must have its own cold key");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let error = runtime.block_on(async {
        let (mut verifier, mut state) = cache_test_verifier();
        let verification = verify(
            &mut verifier,
            Request::Mempool {
                transaction: tx.clone().into(),
                height,
            },
        );
        tokio::pin!(verification);
        assert!(futures::poll!(&mut verification).is_pending());
        let _response = state
            .expect_request_that(|req| {
                matches!(
                    req,
                    zakura_state::Request::CheckBestChainTipNullifiersAndAnchors(_)
                )
            })
            .await
            .unwrap();
        tokio::time::advance(super::super::UTXO_LOOKUP_TIMEOUT + std::time::Duration::from_secs(1))
            .await;
        verification.await.unwrap_err()
    });
    let expected = TransactionError::from(BoxError::from(tower::timeout::error::Elapsed::new()));
    assert_eq!(
        error, expected,
        "preserve the state service timeout conversion"
    );
    assert_eq!(
        zakura_test::MULTI_THREADED_RUNTIME.block_on(async { proof_calls(&tx, upgrade) }),
        before,
        "state timeouts must not dispatch proofs"
    );
}

/// Run alone in a release test executable on Linux, with four Tokio and Rayon threads.
///
/// Repeated valid fixtures model cold verification bursts, not unique network admissions.
/// Actual proof submissions are reported because later requests can hit the bundle cache.
#[cfg(feature = "internal-bench")]
#[test]
#[ignore = "requires an isolated host; run this test alone in release mode"]
#[allow(clippy::print_stdout, clippy::assertions_on_constants)]
fn rejected_anchor_verification_load() {
    use futures::{stream::FuturesUnordered, StreamExt};
    use std::time::Instant;

    // Compile the harness on every platform so local checks cover its types,
    // while keeping execution on Linux for /proc CPU accounting.
    // This is intentionally a runtime guard: a const assertion would prevent
    // the benchmark from compiling on other platforms.
    assert!(
        cfg!(target_os = "linux"),
        "benchmark CPU accounting requires Linux"
    );
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
            let mempool: CacheTestMempool = MockService::build().for_prop_tests();
            setup_tx.send(mempool).ok().unwrap();
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
#[cfg(feature = "internal-bench")]
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
