//! Small mock-state tests for lookup scheduling and output ordering.

use super::*;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tower::{timeout::Timeout, util::BoxService};

use crate::transaction::{MAX_CONCURRENT_BLOCK_UTXO_LOOKUPS, UTXO_LOOKUP_TIMEOUT};

type LookupVerifier<S> = Verifier<
    S,
    Buffer<BoxService<mempool::Request, mempool::Response, BoxError>, mempool::Request>,
>;

/// Give each input a distinct value so an output-order regression is observable.
fn fixture(count: usize, known_count: usize) -> (Request, Vec<transparent::Utxo>) {
    let mut inputs = Vec::new();
    let mut utxos = Vec::new();
    let mut known_utxos = HashMap::new();
    for index in 0..count {
        let (input, _, known) = mock_transparent_transfer(
            Height(1),
            true,
            u32::try_from(index).expect("test input count fits in u32"),
            Amount::try_from(10_000 + i64::try_from(index).unwrap()).unwrap(),
        );
        utxos.push(known.values().next().unwrap().utxo.clone());
        inputs.push(input);
        if index < known_count {
            known_utxos.extend(known);
        }
    }
    let transaction = Arc::new(Transaction::V5 {
        inputs,
        outputs: vec![],
        lock_time: LockTime::unlocked(),
        expiry_height: Height(2),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        network_upgrade: NetworkUpgrade::Nu5,
    });
    let request = Request::Block {
        transaction_hash: transaction.hash(),
        transaction,
        known_utxos: Arc::new(known_utxos),
        known_outpoint_hashes: Arc::new(HashSet::new()),
        height: Height(2),
        time: DateTime::<Utc>::MAX_UTC,
    };
    (request, utxos)
}

#[tokio::test]
async fn block_lookups_are_bounded_and_preserve_input_order() {
    timeout(test_timeout(), async {
        const KNOWN: usize = 2;
        let count = KNOWN + MAX_CONCURRENT_BLOCK_UTXO_LOOKUPS + 3;
        let (request, utxos) = fixture(count, KNOWN);
        let (requests, mut received) = mpsc::unbounded_channel();
        let state = service_fn(move |request| {
            let zakura_state::Request::AwaitUtxo(outpoint) = request else {
                panic!("block lookups must use AwaitUtxo")
            };
            let (send, receive) = oneshot::channel();
            requests.send((outpoint, send)).unwrap();
            async move { receive.await.unwrap() }
        });
        let lookup = tokio::spawn(LookupVerifier::<_>::spent_utxos(
            request.transaction(),
            request,
            Timeout::new(state, UTXO_LOOKUP_TIMEOUT),
            None,
        ));

        let mut seen = HashSet::new();
        for batch_size in [MAX_CONCURRENT_BLOCK_UTXO_LOOKUPS, 3] {
            let mut batch = Vec::new();
            for _ in 0..batch_size {
                let (outpoint, send) = received.recv().await.unwrap();
                assert!(usize::try_from(outpoint.index).unwrap() >= KNOWN);
                assert!(seen.insert(outpoint));
                batch.push((outpoint, send));
            }
            assert!(
                received.try_recv().is_err(),
                "lookup window must remain bounded"
            );
            // Finish later inputs first; returned outputs must still follow input order.
            for (outpoint, send) in batch.into_iter().rev() {
                send.send(Ok(zakura_state::Response::Utxo(
                    utxos[usize::try_from(outpoint.index).unwrap()].clone(),
                )))
                .unwrap();
            }
        }
        let (spent, outputs, mempool_outpoints) = lookup.await.unwrap().unwrap();
        assert_eq!(seen.len(), count - KNOWN);
        assert_eq!(spent.len(), count);
        assert_eq!(
            outputs,
            utxos
                .iter()
                .map(|utxo| utxo.output.clone())
                .collect::<Vec<_>>()
        );
        assert!(mempool_outpoints.is_empty());
    })
    .await
    .expect("lookup scheduling must complete within the test timeout");
}

#[tokio::test]
async fn block_lookup_error_cancels_pending_work() {
    timeout(test_timeout(), async {
        let (request, _) = fixture(MAX_CONCURRENT_BLOCK_UTXO_LOOKUPS + 1, 0);
        let (requests, mut received) = mpsc::unbounded_channel();
        let state = service_fn(move |_| {
            let (send, receive) = oneshot::channel();
            requests.send(send).unwrap();
            async move { receive.await.unwrap() }
        });
        let lookup = tokio::spawn(LookupVerifier::<_>::spent_utxos(
            request.transaction(),
            request,
            Timeout::new(state, UTXO_LOOKUP_TIMEOUT),
            None,
        ));
        let mut pending = Vec::new();
        for _ in 0..MAX_CONCURRENT_BLOCK_UTXO_LOOKUPS {
            pending.push(received.recv().await.unwrap());
        }
        pending
            .pop()
            .unwrap()
            .send(Err("lookup failed".into()))
            .unwrap();
        let error = lookup.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("lookup failed"));
        assert!(pending.iter().all(oneshot::Sender::is_closed));
        assert!(
            received.recv().await.is_none(),
            "no further lookup should be dispatched"
        );
    })
    .await
    .expect("failed lookups must cancel within the test timeout");
}

#[tokio::test(start_paused = true)]
async fn block_lookup_timeout_preserves_missing_input_error() {
    let (request, _) = fixture(2, 0);
    let state =
        service_fn(|_| futures::future::pending::<Result<zakura_state::Response, BoxError>>());
    let error = LookupVerifier::<_>::spent_utxos(
        request.transaction(),
        request,
        Timeout::new(state, Duration::from_secs(1)),
        None,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, TransactionError::TransparentInputNotFound));
}
