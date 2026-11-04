//! Tests for block-level flushes of the shielded batch verifiers.

use std::sync::Arc;

use super::{key, BlockFlush, Pending, Progress, Queueing, Verifier, BLOCKS};

/// Returns how many guards of `pending`'s block have not been released.
fn outstanding(pending: &Pending) -> usize {
    pending
        .lock()
        .expect("test progress mutex is never poisoned")
        .pending
}

#[test]
fn the_last_release_returns_the_queued_verifiers() {
    let mut progress = Progress {
        pending: 2,
        queued: Verifier::Sapling.bit() | Verifier::Halo2Nu6_3Onward.bit(),
    };

    assert_eq!(progress.release(), None);
    assert_eq!(
        progress.release(),
        Some(Verifier::Sapling.bit() | Verifier::Halo2Nu6_3Onward.bit())
    );
}

#[test]
fn verifier_bits_are_distinct() {
    let all = Verifier::ALL.iter().fold(0, |bits, verifier| {
        assert_eq!(bits & verifier.bit(), 0, "{verifier:?} shares a bit");
        bits | verifier.bit()
    });
    assert_eq!(all.count_ones() as usize, Verifier::ALL.len());
}

#[test]
fn guards_exist_only_for_registered_blocks_and_inside_a_scope() {
    let context = Arc::new(());
    assert!(Queueing::for_transaction(&context).is_none());
    assert!(Queueing::for_request().is_none());

    let registration = BlockFlush::register(&context, 1);
    let transaction = Queueing::for_transaction(&context).expect("the block was just registered");
    assert!(Queueing::for_request().is_none());
    let request = transaction.scope(Queueing::for_request);
    assert!(request.is_some());

    drop(request);
    drop(transaction);
    drop(registration);
    assert!(Queueing::for_transaction(&context).is_none());
    assert!(!BLOCKS
        .lock()
        .expect("test registry mutex is never poisoned")
        .contains_key(&key(&context)));
}

#[test]
fn flush_waits_for_late_transactions_and_unqueued_requests() {
    zakura_test::MULTI_THREADED_RUNTIME.block_on(async {
        let context = Arc::new(());
        let _registration = BlockFlush::register(&context, 2);

        let first = Queueing::for_transaction(&context).expect("the block was just registered");
        let pending = first.0.clone();
        let first_request = first
            .scope(Queueing::for_request)
            .expect("built inside the transaction scope");
        drop(first);
        assert_eq!(
            outstanding(&pending),
            2,
            "second transaction and first request"
        );

        // The first request reaches its queue before the other transaction is dispatched.
        first_request.enqueued(Verifier::Ed25519);
        assert_eq!(outstanding(&pending), 1, "second transaction");

        // A request still waiting for queue capacity holds the flush back.
        let last = Queueing::for_transaction(&context).expect("the block is still registered");
        let waiting = last
            .scope(Queueing::for_request)
            .expect("built inside the transaction scope");
        drop(last);
        assert_eq!(outstanding(&pending), 1, "the waiting request");
        assert_eq!(
            pending
                .lock()
                .expect("test progress mutex is never poisoned")
                .queued,
            Verifier::Ed25519.bit()
        );

        // Releasing it flushes the real Ed25519 worker, on the runtime its global verifier uses.
        waiting.enqueued(Verifier::Ed25519);
        assert_eq!(outstanding(&pending), 0);
    });
}

#[test]
fn a_dropped_block_leaves_its_guards_releasable() {
    let context = Arc::new(());
    let registration = BlockFlush::register(&context, 2);
    let transaction = Queueing::for_transaction(&context).expect("the block was just registered");
    let request = transaction
        .scope(Queueing::for_request)
        .expect("built inside the transaction scope");
    let pending = transaction.0.clone();

    // A block abandoned mid-dispatch unregisters; its outstanding guards still release cleanly
    // and, with nothing queued, flush nothing.
    drop(registration);
    assert!(Queueing::for_transaction(&context).is_none());
    drop(transaction);
    drop(request);
    assert_eq!(Arc::strong_count(&pending), 1);
    assert_eq!(
        outstanding(&pending),
        1,
        "the transaction that was never dispatched"
    );
}
