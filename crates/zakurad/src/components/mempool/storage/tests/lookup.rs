//! TXID selection tests and a manual scaling benchmark.

use std::{collections::HashSet, hint::black_box, time::Instant};

use zakura_chain::transaction::{UnminedTx, MEMPOOL_TRANSACTION_COST_THRESHOLD};

use super::super::{config::Config, fixtures::TxFactory, Storage};

#[test]
fn same_effects_lookup_returns_only_requested_stored_transactions() {
    let mut factory = TxFactory::new();
    let mut storage = Storage::new(&Config::default());
    let transactions = [factory.tx(10_000), factory.tx(10_000), factory.tx(10_000)];
    let ids = transactions.each_ref().map(|tx| tx.transaction.id());
    for tx in transactions {
        storage.insert(tx, Vec::new(), None).unwrap();
    }
    let missing = factory.tx(10_000).transaction.id().mined_id();
    let requested = HashSet::from([ids[0].mined_id(), missing, ids[2].mined_id()]);
    let found: HashSet<_> = storage
        .transactions_same_effects(requested)
        .map(UnminedTx::id)
        .collect();
    assert_eq!(found, HashSet::from([ids[0], ids[2]]));
    assert_eq!(storage.transactions_same_effects(HashSet::new()).count(), 0);
    assert_eq!(
        storage
            .transactions_same_effects(HashSet::from([missing]))
            .count(),
        0
    );
}

/// Compares selection cost at realistic pool sizes. Run the same harness on
/// control and candidate; request-set cloning is included in both timings.
#[test]
#[ignore = "manual performance measurement; run with --release --ignored --nocapture"]
#[allow(clippy::print_stdout, reason = "manual benchmark results")]
fn benchmark_mempool_txid_lookup() {
    const ITERATIONS: u32 = 500;
    const SAMPLES: usize = 21;
    let config = Config::default();
    let default_count = config.tx_cost_limit / MEMPOOL_TRANSACTION_COST_THRESHOLD;
    let mut factory = TxFactory::new();
    for count in [0, default_count / 8, default_count] {
        let mut storage = Storage::new(&config);
        let mut stored = Vec::new();
        for _ in 0..count {
            let tx = factory.tx(10_000);
            stored.push(tx.transaction.id().mined_id());
            storage.insert(tx, Vec::new(), None).unwrap();
        }
        let missing = factory.tx(10_000).transaction.id().mined_id();
        let hit = stored.first().copied().unwrap_or(missing);
        let mixed = stored.iter().copied().take(15).chain([missing]).collect();
        for (name, requested) in [
            ("hit", HashSet::from([hit])),
            ("miss", HashSet::from([missing])),
            ("mixed", mixed),
        ] {
            let expected = storage.transactions_same_effects(requested.clone()).count();
            let mut samples = Vec::with_capacity(SAMPLES);
            for _ in 0..SAMPLES {
                let start = Instant::now();
                for _ in 0..ITERATIONS {
                    let found = storage
                        .transactions_same_effects(black_box(requested.clone()))
                        .count();
                    assert_eq!(black_box(found), expected);
                }
                samples.push(start.elapsed() / ITERATIONS);
            }
            samples.sort();
            println!(
                "lookup transactions={count} case={name} median_ns={} min_ns={} max_ns={}",
                samples[SAMPLES / 2].as_nanos(),
                samples[0].as_nanos(),
                samples[SAMPLES - 1].as_nanos(),
            );
        }
    }
}
