//! Release-mode lower-bound benchmark for non-finalized chain snapshot cloning.

#![allow(clippy::print_stdout)]

use std::{collections::HashMap, env, hint::black_box, sync::Arc, time::Instant};

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::Height,
    orchard, transaction, transparent,
};
use zakura_state::TransactionLocation;

const DEFAULT_BLOCKS: usize = 1_000;
const DEFAULT_TRANSACTIONS_PER_BLOCK: usize = 165;
const DEFAULT_ORCHARD_NULLIFIERS_PER_BLOCK: usize = 330;
const DEFAULT_SAMPLES: usize = 100;

fn main() {
    let blocks = env_usize("ZAKURA_CHAIN_CLONE_BLOCKS", DEFAULT_BLOCKS);
    let transactions_per_block = env_usize(
        "ZAKURA_CHAIN_CLONE_TRANSACTIONS_PER_BLOCK",
        DEFAULT_TRANSACTIONS_PER_BLOCK,
    );
    let nullifiers_per_block = env_usize(
        "ZAKURA_CHAIN_CLONE_NULLIFIERS_PER_BLOCK",
        DEFAULT_ORCHARD_NULLIFIERS_PER_BLOCK,
    );
    let samples = env_usize("ZAKURA_CHAIN_CLONE_SAMPLES", DEFAULT_SAMPLES);

    let transaction_count = blocks
        .checked_mul(transactions_per_block)
        .expect("benchmark transaction count must fit in usize");
    let nullifier_count = blocks
        .checked_mul(nullifiers_per_block)
        .expect("benchmark nullifier count must fit in usize");
    let transactions = transaction_locations(transaction_count, transactions_per_block);
    let nullifiers = orchard_nullifiers(nullifier_count);
    let contextual_outputs = contextual_outputs(blocks, transactions_per_block);
    let shared_contextual_outputs: Vec<_> =
        contextual_outputs.iter().cloned().map(Arc::new).collect();
    let created_utxos: HashMap<_, _> = contextual_outputs
        .iter()
        .flat_map(|outputs| {
            outputs
                .iter()
                .map(|(outpoint, utxo)| (*outpoint, utxo.clone()))
        })
        .collect();

    let mut dominant_index_timings = Vec::with_capacity(samples);
    let mut created_utxo_timings = Vec::with_capacity(samples);
    let mut contextual_output_timings = Vec::with_capacity(samples);
    let mut shared_output_timings = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        let cloned_transactions = black_box(&transactions).clone();
        let cloned_nullifiers = black_box(&nullifiers).clone();
        dominant_index_timings.push(start.elapsed());
        black_box((cloned_transactions, cloned_nullifiers));

        let start = Instant::now();
        let cloned_created_utxos = black_box(&created_utxos).clone();
        created_utxo_timings.push(start.elapsed());
        black_box(cloned_created_utxos);

        let start = Instant::now();
        let cloned_outputs = black_box(&contextual_outputs).clone();
        contextual_output_timings.push(start.elapsed());
        black_box(cloned_outputs);

        let start = Instant::now();
        let cloned_outputs = black_box(&shared_contextual_outputs).clone();
        shared_output_timings.push(start.elapsed());
        black_box(cloned_outputs);
    }
    print_timings(
        "chain_dominant_index_clone",
        blocks,
        transaction_count,
        nullifier_count,
        dominant_index_timings,
    );
    print_timings(
        "created_utxo_index_clone",
        blocks,
        transaction_count,
        nullifier_count,
        created_utxo_timings,
    );
    print_timings(
        "contextual_output_map_clone",
        blocks,
        transaction_count,
        nullifier_count,
        contextual_output_timings,
    );
    print_timings(
        "shared_contextual_output_map_clone",
        blocks,
        transaction_count,
        nullifier_count,
        shared_output_timings,
    );
}

fn print_timings(
    operation: &str,
    blocks: usize,
    transaction_count: usize,
    nullifier_count: usize,
    mut timings: Vec<std::time::Duration>,
) {
    timings.sort_unstable();
    let median = timings[timings.len() / 2];
    let p95 = timings[timings.len().saturating_sub(1) * 95 / 100];
    println!(
        "operation={operation} blocks={blocks} transactions={transaction_count} \
         orchard_nullifiers={nullifier_count} samples={} median_ns={} p95_ns={}",
        timings.len(),
        median.as_nanos(),
        p95.as_nanos(),
    );
}

fn transaction_locations(
    count: usize,
    transactions_per_block: usize,
) -> HashMap<transaction::Hash, TransactionLocation> {
    (0..count)
        .map(|index| {
            let unique = u64::try_from(index).expect("benchmark count fits in u64");
            let mut hash = [0; 32];
            hash[..8].copy_from_slice(&unique.to_le_bytes());
            let height = u32::try_from(index / transactions_per_block)
                .expect("benchmark height fits in u32");
            let tx_index = index % transactions_per_block;
            (
                transaction::Hash(hash),
                TransactionLocation::from_usize(Height(height), tx_index),
            )
        })
        .collect()
}

fn orchard_nullifiers(count: usize) -> HashMap<orchard::Nullifier, ()> {
    (1..=count)
        .map(|index| {
            let value = u64::try_from(index).expect("benchmark count fits in u64");
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(&value.to_le_bytes());
            let nullifier = orchard::Nullifier::try_from(bytes)
                .expect("small integers are valid Pallas values");
            (nullifier, ())
        })
        .collect()
}

fn contextual_outputs(
    blocks: usize,
    transactions_per_block: usize,
) -> Vec<HashMap<transparent::OutPoint, transparent::OrderedUtxo>> {
    let value: Amount<NonNegative> = 1.try_into().expect("one zatoshi is valid");
    let output = transparent::Output::new(value, transparent::Script::new(&[]));

    (0..blocks)
        .map(|block_index| {
            (0..transactions_per_block)
                .map(|transaction_index| {
                    let unique = block_index
                        .checked_mul(transactions_per_block)
                        .and_then(|index| index.checked_add(transaction_index))
                        .expect("benchmark output index fits in usize");
                    let unique = u64::try_from(unique).expect("benchmark output index fits in u64");
                    let mut hash = [0; 32];
                    hash[..8].copy_from_slice(&unique.to_le_bytes());
                    let outpoint = transparent::OutPoint {
                        hash: transaction::Hash(hash),
                        index: 0,
                    };
                    let height = u32::try_from(block_index).expect("benchmark height fits in u32");
                    let ordered = transparent::OrderedUtxo::from_utxo(
                        transparent::Utxo::new(output.clone(), Height(height), false),
                        transaction_index,
                    );
                    (outpoint, ordered)
                })
                .collect()
        })
        .collect()
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .map(|value| value.parse().expect("benchmark setting must be a usize"))
        .unwrap_or(default)
}
