//! Tests for the script verifier service.

use std::sync::Arc;

use tower::ServiceExt;
use zakura_chain::{
    block::Block, parameters::NetworkUpgrade, serialization::ZcashDeserialize, transparent,
};
use zakura_script::CachedFfiTransaction;

use super::{Request, Verifier};

/// Calling the script `Verifier` with an `input_index` past the end of the transaction's
/// inputs must return an error instead of panicking.
#[tokio::test]
async fn verifier_returns_error_for_out_of_range_input_index() {
    let _init_guard = zakura_test::init();

    // Use a v4 mainnet block whose second transaction has at least one transparent input.
    let block = Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_419199_BYTES[..])
        .expect("test vector is a valid block");
    let tx = block
        .transactions
        .iter()
        .find(|tx| {
            tx.inputs()
                .iter()
                .any(|i| matches!(i, transparent::Input::PrevOut { .. }))
        })
        .expect("block has a non-coinbase tx")
        .clone();

    // Build matching previous outputs (one per input). Their content does not matter
    // because the verifier should error on the out-of-range index before touching them.
    let previous_outputs: Vec<transparent::Output> = tx
        .inputs()
        .iter()
        .map(|_| transparent::Output {
            value: 0.try_into().expect("zero is a valid amount"),
            lock_script: transparent::Script::new(&[]),
        })
        .collect();

    let cached = Arc::new(
        CachedFfiTransaction::new(
            tx.clone(),
            Arc::new(previous_outputs),
            NetworkUpgrade::Sapling,
        )
        .expect("constructor accepts the test fixture"),
    );

    let out_of_range = tx.inputs().len();
    let result = Verifier
        .oneshot(Request {
            cached_ffi_transaction: cached,
            input_index: out_of_range,
        })
        .await;

    let err = result.expect_err("out-of-range input_index must error, not panic");
    assert!(
        err.to_string().contains("missing input"),
        "unexpected error message: {err}",
    );
}

/// A cancelled caller cannot settle the decoded memory of its queued Rayon job.
#[tokio::test]
async fn cancelled_script_caller_keeps_transferred_block_charge_until_worker_finishes() {
    use std::task::{Context, Waker};

    use tower::Service;
    use zakura_node_services::block_memory::{Admission, DecodedBlockPool};

    let block = Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_419199_BYTES[..])
        .expect("the vector contains a block");
    let index = block
        .transactions
        .iter()
        .position(|transaction| {
            transaction
                .inputs()
                .iter()
                .any(|input| matches!(input, transparent::Input::PrevOut { .. }))
        })
        .expect("the block contains a transparent spend");
    let bytes = block.attributed_memory_size_bytes();
    let ingress = DecodedBlockPool::new(bytes);
    let backend = DecodedBlockPool::new(bytes);
    let owned = ingress.try_admit(block, Admission::WithinLimit).unwrap();
    let transaction = owned.transaction(index).unwrap();
    let previous_outputs = transaction
        .inputs()
        .iter()
        .map(|_| transparent::Output {
            value: 0.try_into().unwrap(),
            lock_script: transparent::Script::new(&[]),
        })
        .collect();
    let cached = CachedFfiTransaction::new(
        transaction,
        Arc::new(previous_outputs),
        NetworkUpgrade::Sapling,
    )
    .expect("the fixture supports the selected upgrade");
    let cached = Arc::new(cached);
    let actual = Arc::downgrade(&cached);
    let mut future = Verifier.call(Request {
        cached_ffi_transaction: cached,
        input_index: 0,
    });

    // The only worker polls the caller, so its queued verification cannot start yet.
    let workers = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();
    workers.install(|| {
        assert!(future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        owned.transfer_to(&backend, Admission::WithinLimit).unwrap();
        drop(owned);
        drop(future);
        assert!(
            actual.upgrade().is_some(),
            "the queued worker still retains the transaction"
        );
        assert_eq!(ingress.reserved_bytes(), 0);
        assert_eq!(
            backend.reserved_bytes(),
            bytes,
            "queued script work must retain its decoded charge"
        );
    });

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let available = backend.capacity().notified();
            tokio::pin!(available);
            available.as_mut().enable();
            if backend.reserved_bytes() == 0 {
                break;
            }
            available.await;
        }
    })
    .await
    .expect("the Rayon job finishes and releases its payload");
    assert!(
        actual.upgrade().is_none(),
        "settlement must follow actual payload release"
    );
}
