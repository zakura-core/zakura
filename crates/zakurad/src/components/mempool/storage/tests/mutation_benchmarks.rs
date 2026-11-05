//! Control/candidate measurements of post-verification storage mutations.

use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use zakura_chain::{
    block::{Block, MAX_BLOCK_BYTES},
    serialization::{CompactSizeMessage, ZcashDeserializeInto, ZcashSerialize},
    transaction::{VerifiedUnminedTx, MEMPOOL_TRANSACTION_COST_THRESHOLD},
    transparent::OutPoint,
};

use super::super::{
    config::Config, fixtures::TxFactory, verified_set::MAX_MEMPOOL_ANCESTORS, Storage,
};

const SAMPLES: usize = 21;

struct Fixture {
    storage: Storage,
    package: Vec<(VerifiedUnminedTx, Vec<OutPoint>)>,
}

impl Fixture {
    fn new(factory: &mut TxFactory, count: usize, ancestors: usize) -> Self {
        let mut storage = Storage::new(&Config {
            tx_cost_limit: (u64::try_from(count).unwrap() + 1) * MEMPOOL_TRANSACTION_COST_THRESHOLD,
            ..Default::default()
        });
        for index in 0..count - ancestors - 1 {
            let fee = [10_000, 20_000, 50_000][index % 3];
            storage.insert(factory.tx(fee), Vec::new(), None).unwrap();
        }
        let mut package: Vec<(VerifiedUnminedTx, Vec<OutPoint>)> = Vec::new();
        for _ in 0..=ancestors {
            let spent = package
                .last()
                .map(|(parent, _)| OutPoint::from_usize(parent.transaction.id().mined_id(), 0))
                .into_iter()
                .collect::<Vec<_>>();
            let tx = factory.tx_with(20_000, 1, 0);
            storage.insert(tx.clone(), spent.clone(), None).unwrap();
            package.push((tx, spent));
        }
        assert_eq!(storage.transaction_count(), count);
        Self { storage, package }
    }

    fn remove_leaf(&mut self) {
        let leaf = self.package.last().unwrap().0.transaction.id().mined_id();
        assert_eq!(self.storage.verified.remove_all([leaf]).len(), 1);
    }

    /// Includes admission checks and index maintenance; cloning and removal
    /// of the new leaf are outside the timed section.
    fn insert_leaf(&mut self) -> Duration {
        let (tx, spent) = self.package.last().unwrap();
        let tx = tx.clone();
        let spent = spent.clone();
        let start = Instant::now();
        let result = black_box(self.storage.insert(black_box(tx), black_box(spent), None));
        let elapsed = start.elapsed();
        assert!(result.is_ok());
        self.remove_leaf();
        elapsed
    }

    /// Includes descendant removal, index updates, and metrics. Restoring the
    /// package and dropping the returned IDs are outside the timed section.
    fn delete_package(&mut self) -> Duration {
        let root = self.package[0].0.transaction.id().mined_id();
        let start = Instant::now();
        let removed = black_box(self.storage.verified.remove_all(black_box([root])));
        let elapsed = start.elapsed();
        assert_eq!(removed.len(), self.package.len());
        for (tx, spent) in &self.package {
            self.storage
                .insert(tx.clone(), spent.clone(), None)
                .unwrap();
        }
        elapsed
    }

    /// Models mined-parent removal while retaining its unmined descendants.
    fn confirm_parent(&mut self) -> Duration {
        let root = self.package[0].0.transaction.id().mined_id();
        let mined = [root].into_iter().collect();
        let start = Instant::now();
        self.storage.clear_mined_dependencies(black_box(&mined));
        let removed = black_box(self.storage.verified.remove_all(black_box([root])));
        let elapsed = start.elapsed();
        assert_eq!(removed.len(), 1);

        // Restore the original graph, including the children that stayed in
        // the pool. All cleanup and reinsertion are outside the timed section.
        self.storage.verified.remove_all(
            self.package
                .iter()
                .map(|(tx, _)| tx.transaction.id().mined_id()),
        );
        for (tx, spent) in &self.package {
            self.storage
                .insert(tx.clone(), spent.clone(), None)
                .unwrap();
        }
        elapsed
    }
}

/// Run the same harness against the base and candidate production code.
/// The pool size is measured after insertion and before deletion.
#[test]
#[ignore = "manual performance measurement; run with --release --ignored --nocapture"]
#[allow(clippy::print_stdout, reason = "manual benchmark results")]
fn benchmark_mempool_mining_index_mutations() {
    const ITERATIONS: u32 = 128;
    let default_count =
        usize::try_from(Config::default().tx_cost_limit / MEMPOOL_TRANSACTION_COST_THRESHOLD)
            .unwrap();
    let mut factory = TxFactory::new();
    for count in [default_count / 8, default_count] {
        for ancestors in 0..=MAX_MEMPOOL_ANCESTORS {
            let mut fixture = Fixture::new(&mut factory, count, ancestors);
            for operation in ["ingestion", "deletion", "confirmation"] {
                if operation == "ingestion" {
                    fixture.remove_leaf();
                }
                let mut sample = || match operation {
                    "ingestion" => fixture.insert_leaf(),
                    "deletion" => fixture.delete_package(),
                    "confirmation" => fixture.confirm_parent(),
                    _ => unreachable!("the benchmark enumerates its operations above"),
                };
                for _ in 0..ITERATIONS {
                    let _ = sample();
                }
                let mut samples = Vec::with_capacity(SAMPLES);
                for _ in 0..SAMPLES {
                    let duration: Duration = (0..ITERATIONS).map(|_| sample()).sum();
                    samples.push(duration / ITERATIONS);
                }
                samples.sort();
                println!(
                    "mutation transactions={count} ancestors={ancestors} operation={operation} \
                     median_ns={} min_ns={} max_ns={}",
                    samples[SAMPLES / 2].as_nanos(),
                    samples[0].as_nanos(),
                    samples[SAMPLES - 1].as_nanos(),
                );
                if operation == "ingestion" {
                    let (tx, spent) = fixture.package.last().unwrap();
                    fixture
                        .storage
                        .insert(tx.clone(), spent.clone(), None)
                        .unwrap();
                }
                assert_eq!(fixture.storage.transaction_count(), count);
            }
        }
    }
}

/// A block-shaped payload exactly at the serialized block size limit. The
/// genesis header/coinbase and placeholder proofs are only fixture data;
/// contextual and cryptographic block validation are outside these timings.
fn full_block(factory: &mut TxFactory) -> (Block, Vec<VerifiedUnminedTx>) {
    let mut block: Block = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let limit = usize::try_from(MAX_BLOCK_BYTES).unwrap();
    let header_bytes = block.header.zcash_serialized_size();
    let mut transaction_bytes = block.transactions[0].zcash_serialized_size();
    let block_size = |count: usize, bytes: usize| {
        header_bytes
            + CompactSizeMessage::try_from(count)
                .unwrap()
                .zcash_serialized_size()
            + bytes
    };
    let mut members: Vec<VerifiedUnminedTx> = Vec::new();
    loop {
        let tx = factory.tx(20_000);
        if block_size(members.len() + 2, transaction_bytes + tx.transaction.size()) > limit {
            break;
        }
        transaction_bytes += tx.transaction.size();
        members.push(tx);
    }
    let padding = limit - block_size(members.len() + 1, transaction_bytes);
    let last = members.last_mut().unwrap();
    let padded = factory.tx_with(20_000, 0, padding);
    assert_eq!(padded.transaction.size(), last.transaction.size() + padding);
    *last = padded;
    block.transactions.extend(
        members
            .iter()
            .map(|tx| tx.transaction.transaction().clone()),
    );
    assert_eq!(block.zcash_serialized_size(), limit);
    (block, members)
}

/// Measures the full mined-transaction cleanup path for a serialized 2 MB
/// block, including the coinbase TXID that is absent from the mempool.
#[test]
#[ignore = "manual performance measurement; run with --release --ignored --nocapture"]
#[allow(clippy::print_stdout, reason = "manual benchmark results")]
fn benchmark_mempool_full_block_deletion() {
    const BLOCK_ITERATIONS: u32 = 16;
    let default_count =
        usize::try_from(Config::default().tx_cost_limit / MEMPOOL_TRANSACTION_COST_THRESHOLD)
            .unwrap();
    let mut factory = TxFactory::new();
    for count in [default_count / 8, default_count] {
        let (block, members) = full_block(&mut factory);
        assert!(members.len() <= count);
        let block_cost: u64 = members.iter().map(VerifiedUnminedTx::cost).sum();
        let background_count = count - members.len();
        let mut storage = Storage::new(&Config {
            tx_cost_limit: u64::try_from(background_count).unwrap()
                * MEMPOOL_TRANSACTION_COST_THRESHOLD
                + block_cost,
            ..Default::default()
        });
        for _ in 0..background_count {
            storage
                .insert(factory.tx(10_000), Vec::new(), None)
                .unwrap();
        }
        for tx in &members {
            storage.insert(tx.clone(), Vec::new(), None).unwrap();
        }
        let mined = block
            .transactions
            .iter()
            .map(|tx| tx.unmined_id().mined_id())
            .collect();
        let mut sample = || {
            let transactions = block.transactions.clone();
            let start = Instant::now();
            storage.clear_mined_dependencies(black_box(&mined));
            let removed = black_box(
                storage.reject_and_remove_same_effects(black_box(&mined), black_box(transactions)),
            );
            let elapsed = start.elapsed();
            assert_eq!(removed.mined.len(), members.len());
            assert!(removed.invalidated.is_empty());
            assert_eq!(storage.transaction_count(), background_count);
            storage.chain_rejected_same_effects.clear();
            for tx in &members {
                storage.insert(tx.clone(), Vec::new(), None).unwrap();
            }
            assert_eq!(storage.transaction_count(), count);
            elapsed
        };
        for _ in 0..BLOCK_ITERATIONS {
            let _ = sample();
        }
        let mut samples = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let duration: Duration = (0..BLOCK_ITERATIONS).map(|_| sample()).sum();
            samples.push(duration / BLOCK_ITERATIONS);
        }
        samples.sort();
        println!(
            "full_block transactions={count} block_bytes={} block_transactions={} removed={} \
             median_ns={} min_ns={} max_ns={}",
            block.zcash_serialized_size(),
            block.transactions.len(),
            members.len(),
            samples[SAMPLES / 2].as_nanos(),
            samples[0].as_nanos(),
            samples[SAMPLES - 1].as_nanos(),
        );
    }
}
