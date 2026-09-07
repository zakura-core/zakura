//! Regression checks for the block-owned resolver.

use super::*;
use crate::transaction::BlockUtxos;

fn fixture(
    input_count: u32,
    known_every: u32,
) -> (
    Block,
    HashMap<transparent::OutPoint, transparent::OrderedUtxo>,
    HashMap<transparent::OutPoint, transparent::Utxo>,
) {
    let mut inputs = Vec::new();
    let mut known = HashMap::new();
    let mut expected = HashMap::new();
    for index in 0..input_count {
        let (input, _, utxos) = mock_transparent_transfer(
            Height(1),
            true,
            index,
            Amount::try_from(u64::from(index) + 1).unwrap(),
        );
        inputs.push(input);
        for (outpoint, utxo) in utxos {
            expected.insert(outpoint, utxo.utxo.clone());
            if known_every != 0 && index % known_every == 0 {
                known.insert(outpoint, utxo);
            }
        }
    }
    let mut block: Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let second_half = inputs.split_off(inputs.len() / 2);
    block.transactions = [inputs, second_half]
        .into_iter()
        .map(|inputs| {
            Arc::new(Transaction::V5 {
                network_upgrade: NetworkUpgrade::Nu5,
                inputs,
                outputs: vec![transparent::Output {
                    value: Amount::try_from(1).unwrap(),
                    lock_script: transparent::Script::new(&[0x51]),
                }],
                lock_time: LockTime::unlocked(),
                expiry_height: Height(2_000_000),
                sapling_shielded_data: None,
                orchard_shielded_data: None,
            })
        })
        .collect();
    (block, known, expected)
}

fn request(
    tx: Arc<Transaction>,
    known: Arc<HashMap<transparent::OutPoint, transparent::OrderedUtxo>>,
    resolver: Option<BlockUtxos>,
) -> Request {
    Request::Block {
        transaction_hash: tx.hash(),
        transaction: tx,
        known_utxos: known,
        known_outpoint_hashes: Arc::new(HashSet::new()),
        utxo_resolver: resolver,
        height: NetworkUpgrade::Nu5
            .activation_height(&Network::Mainnet)
            .unwrap(),
        time: Utc::now(),
    }
}

#[tokio::test]
async fn block_resolver_shares_bounded_batches_and_preserves_v5_sighashes() {
    for input_count in [0, 1, 2, 63, 64, 65, 257, 513] {
        for known_every in [0, 1, 3] {
            let (mut block, known, expected) = fixture(input_count, known_every);
            // A second consumer of the same transaction must not repeat its lookups.
            block.transactions.push(block.transactions[1].clone());
            let (requests, mut received) = tokio::sync::mpsc::unbounded_channel();
            let state = tower::service_fn(move |request| {
                let zakura_state::Request::AwaitUtxos(outpoints) = request else {
                    panic!("the shared resolver must use batched state requests")
                };
                let (send, receive) = tokio::sync::oneshot::channel();
                requests.send((outpoints, send)).unwrap();
                async move { receive.await.unwrap() }
            });
            let resolver = BlockUtxos::for_block(&block, &known, state.clone());
            let mut remaining = expected.len() - known.len();
            assert_eq!(resolver.is_none(), remaining == 0);
            let known = Arc::new(known);
            let lookups = futures::future::join_all(block.transactions.iter().map(|tx| {
                Verifier::<
                    _,
                    tower::util::BoxCloneService<mempool::Request, mempool::Response, BoxError>,
                >::spent_utxos(
                    tx.clone(),
                    request(tx.clone(), known.clone(), resolver.clone()),
                    tower::timeout::Timeout::new(state.clone(), super::super::UTXO_LOOKUP_TIMEOUT),
                    None,
                )
            }));
            futures::pin_mut!(lookups);
            let mut seen = HashSet::new();
            while remaining > 0 {
                tokio::task::yield_now().await;
                assert!(futures::poll!(&mut lookups).is_pending());
                let mut batches = Vec::new();
                while let Ok(batch) = received.try_recv() {
                    batches.push(batch);
                }
                assert_eq!(batches.len(), remaining.div_ceil(64).min(4));
                for (outpoints, send) in batches.into_iter().rev() {
                    assert!(!outpoints.is_empty() && outpoints.len() <= 64);
                    remaining -= outpoints.len();
                    let response = outpoints
                        .into_iter()
                        .rev()
                        .map(|outpoint| {
                            assert!(
                                seen.insert(outpoint),
                                "resolve each external outpoint once per block"
                            );
                            assert!(!known.contains_key(&outpoint));
                            (outpoint, expected[&outpoint].clone())
                        })
                        .collect();
                    send.send(Ok(zakura_state::Response::Utxos(response)))
                        .unwrap();
                }
            }
            let results = timeout(test_timeout(), lookups).await.unwrap();
            for (tx, result) in block.transactions.iter().zip(results) {
                let (utxos, outputs, mempool_outpoints) = result.unwrap();
                let expected_outputs: Vec<_> = tx
                    .spent_outpoints()
                    .map(|outpoint| expected[&outpoint].output.clone())
                    .collect();
                assert_eq!(outputs, expected_outputs);
                assert!(mempool_outpoints.is_empty());
                assert_eq!(utxos.len(), tx.inputs().len());
                let sighash = |outputs| {
                    zakura_chain::transaction::SigHasher::new(
                        tx,
                        NetworkUpgrade::Nu5,
                        Arc::new(outputs),
                    )
                    .unwrap()
                    .sighash(HashType::ALL, None)
                };
                assert_eq!(sighash(outputs), sighash(expected_outputs.clone()));
                if expected_outputs.len() > 1 {
                    let mut reversed = expected_outputs.clone();
                    reversed.reverse();
                    assert_ne!(sighash(reversed), sighash(expected_outputs));
                }
            }
            assert!(received.try_recv().is_err());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn block_resolver_errors_timeout_and_drop_cancel_batches() {
    for failure in ["error", "timeout", "drop", "incomplete"] {
        let (block, known, _) = fixture(513, 0);
        let (requests, mut received) = tokio::sync::mpsc::unbounded_channel();
        let state = tower::service_fn(move |_| {
            let (send, receive) = tokio::sync::oneshot::channel();
            requests.send(send).unwrap();
            async move { receive.await.unwrap() }
        });
        let resolver = BlockUtxos::for_block(&block, &known, state).unwrap();
        let consumer = resolver.clone();
        let mut lookup = Box::pin(async move { consumer.resolve().await });
        assert!(futures::poll!(&mut lookup).is_pending());
        let mut pending = Vec::new();
        while let Ok(send) = received.try_recv() {
            pending.push(send);
        }
        assert_eq!(pending.len(), 4);
        match failure {
            "error" => pending
                .pop()
                .unwrap()
                .send(Err("batch failed".into()))
                .unwrap(),
            "incomplete" => pending
                .pop()
                .unwrap()
                .send(Ok(zakura_state::Response::Utxos(HashMap::new())))
                .unwrap(),
            "timeout" => tokio::time::advance(super::super::UTXO_LOOKUP_TIMEOUT).await,
            "drop" => {}
            _ => unreachable!(),
        }
        if failure != "drop" {
            let error = lookup.as_mut().await.unwrap_err();
            if failure == "error" {
                assert!(error.to_string().contains("batch failed"));
            } else {
                assert_eq!(error, TransactionError::TransparentInputNotFound);
            }
        }
        drop(lookup);
        drop(resolver);
        assert!(pending.iter().all(tokio::sync::oneshot::Sender::is_closed));
        assert!(received.try_recv().is_err());
    }
}

#[tokio::test]
async fn block_resolver_keeps_quick_rejection_and_transaction_results() {
    let (block, known, expected) = fixture(4, 0);
    let expected = Arc::new(expected);
    let state = tower::service_fn(move |request| {
        let response = match request {
            zakura_state::Request::AwaitUtxo(outpoint) => {
                zakura_state::Response::Utxo(expected[&outpoint].clone())
            }
            zakura_state::Request::AwaitUtxos(outpoints) => zakura_state::Response::Utxos(
                outpoints
                    .into_iter()
                    .map(|outpoint| (outpoint, expected[&outpoint].clone()))
                    .collect(),
            ),
            _ => panic!("transparent block verification only needs UTXO lookups"),
        };
        async move { Ok::<_, BoxError>(response) }
    });
    let resolver = BlockUtxos::for_block(&block, &known, state.clone());
    for tx in &block.transactions {
        let baseline = Verifier::new_for_tests(&Network::Mainnet, state.clone())
            .oneshot(request(tx.clone(), Arc::new(known.clone()), None))
            .await;
        let batched = Verifier::new_for_tests(&Network::Mainnet, state.clone())
            .oneshot(request(
                tx.clone(),
                Arc::new(known.clone()),
                resolver.clone(),
            ))
            .await;
        assert!(baseline.is_ok(), "{baseline:?}");
        assert_eq!(batched, baseline);
    }

    let mut invalid = block.clone();
    let Transaction::V5 { inputs, .. } = Arc::make_mut(&mut invalid.transactions[0]) else {
        unreachable!()
    };
    inputs.push(inputs[0].clone());
    let state = tower::service_fn(|_| async {
        Err::<zakura_state::Response, BoxError>("quick rejection queried state".into())
    });
    let resolver = BlockUtxos::for_block(&invalid, &known, state);
    let error = Verifier::new_for_tests(&Network::Mainnet, state)
        .oneshot(request(
            invalid.transactions[0].clone(),
            Arc::new(known),
            resolver,
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        TransactionError::DuplicateTransparentSpend(_)
    ));
}

#[tokio::test]
async fn block_resolver_refills_batches_while_earlier_batches_wait() {
    let (block, known, expected) = fixture(513, 0);
    let (requests, mut received) = tokio::sync::mpsc::unbounded_channel();
    let state = tower::service_fn(move |request| {
        let zakura_state::Request::AwaitUtxos(outpoints) = request else {
            unreachable!()
        };
        let (send, receive) = tokio::sync::oneshot::channel();
        requests.send((outpoints, send)).unwrap();
        async move { receive.await.unwrap() }
    });
    let resolver = BlockUtxos::for_block(&block, &known, state).unwrap();
    let consumer = resolver.clone();
    let mut lookup = Box::pin(async move { consumer.resolve().await });
    assert!(futures::poll!(&mut lookup).is_pending());
    let mut pending = Vec::new();
    while let Ok(batch) = received.try_recv() {
        pending.push(batch);
    }
    assert_eq!(pending.len(), 4);
    for _ in 0..5 {
        let (outpoints, send) = pending.pop().unwrap();
        send.send(Ok(zakura_state::Response::Utxos(
            outpoints
                .into_iter()
                .map(|outpoint| (outpoint, expected[&outpoint].clone()))
                .collect(),
        )))
        .unwrap();
        tokio::task::yield_now().await;
        assert!(futures::poll!(&mut lookup).is_pending());
        pending.push(
            received
                .try_recv()
                .expect("one completed batch must free one slot"),
        );
        assert!(received.try_recv().is_err());
    }
    drop(lookup);
    assert!(
        pending.iter().all(|(_, send)| !send.is_closed()),
        "the block still owns the resolver"
    );
    drop(resolver);
    assert!(pending.iter().all(|(_, send)| send.is_closed()));
}

#[tokio::test(start_paused = true)]
async fn block_resolver_preserves_inner_state_timeout_errors() {
    let (block, known, _) = fixture(1, 0);
    let state = tower::service_fn(|_| {
        futures::future::pending::<Result<zakura_state::Response, BoxError>>()
    });
    let state = tower::timeout::Timeout::new(state, std::time::Duration::from_secs(1));
    let resolver = BlockUtxos::for_block(&block, &known, state).unwrap();
    let lookup = resolver.resolve();
    futures::pin_mut!(lookup);
    assert!(futures::poll!(&mut lookup).is_pending());
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    assert_eq!(
        lookup.await.unwrap_err(),
        TransactionError::TransparentInputNotFound
    );
}
