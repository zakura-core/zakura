//! One-shot benchmark for worst-case transparent remaining-value validation.
//!
//! The 2 MB fixture contains a minimal V5 coinbase plus 26,645 independent
//! one-input, one-output V5 transactions. Each spend transaction serializes to
//! 75 bytes and has a matching non-coinbase OP_TRUE UTXO. This shape is
//! consensus-satisfiable after staging the UTXOs, but the fixture does not
//! construct a valid header commitment, subsidy, or proof of work because this
//! benchmark isolates the contextual remaining-value check.

use std::{
    collections::HashMap,
    env,
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{Block, Height, MAX_BLOCK_BYTES},
    parameters::NetworkUpgrade,
    serialization::{ZcashDeserialize, ZcashSerialize},
    transaction::{self, LockTime, Transaction},
    transparent,
};
use zakura_state::{check::remaining_transaction_value, SemanticallyVerifiedBlock};
use zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES;

const MAX_SPEND_TRANSACTIONS: usize = 26_645;
const MINIMAL_V5_SPEND_TX_BYTES: usize = 75;
const BENCHMARK_HEIGHT: Height = Height(1_687_108);
const OP_TRUE: u8 = 0x51;

fn main() {
    let transaction_counts = env::var("ZAKURA_BENCH_TXS")
        .map(|counts| parse_transaction_counts(&counts))
        .unwrap_or_else(|_| vec![MAX_SPEND_TRANSACTIONS]);

    for transaction_count in transaction_counts {
        assert!(
            (1..=MAX_SPEND_TRANSACTIONS).contains(&transaction_count),
            "benchmark transaction count must fit in the 2 MB block maximum"
        );

        let (block, prepared, spent_utxos) = benchmark_fixture(transaction_count);
        let block_bytes = block.zcash_serialized_size();

        let start = Instant::now();
        black_box(remaining_transaction_value(
            black_box(&prepared),
            black_box(&spent_utxos),
        ))
        .expect("equal input and output values have nonnegative remaining value");
        let elapsed = start.elapsed();

        print_result(transaction_count, block_bytes, elapsed);
    }
}

fn parse_transaction_counts(counts: &str) -> Vec<usize> {
    counts
        .split(',')
        .map(|count| {
            count
                .trim()
                .parse()
                .expect("ZAKURA_BENCH_TXS values must be positive integers")
        })
        .collect()
}

fn benchmark_fixture(
    transaction_count: usize,
) -> (
    Arc<Block>,
    SemanticallyVerifiedBlock,
    HashMap<transparent::OutPoint, transparent::OrderedUtxo>,
) {
    let genesis = Block::zcash_deserialize(&BLOCK_MAINNET_GENESIS_BYTES[..])
        .expect("mainnet genesis block must deserialize");
    let coinbase = Arc::new(minimal_v5_coinbase());
    let mut transactions = Vec::with_capacity(transaction_count + 1);
    let mut spent_utxos = HashMap::with_capacity(transaction_count);

    transactions.push(coinbase);

    for index in 0..transaction_count {
        let outpoint = synthetic_outpoint(index);
        let transaction = minimal_v5_spend(outpoint);

        assert_eq!(
            transaction.zcash_serialized_size(),
            MINIMAL_V5_SPEND_TX_BYTES,
            "benchmark transaction encoding changed"
        );

        spent_utxos.insert(
            outpoint,
            transparent::OrderedUtxo::from_utxo(
                transparent::Utxo::new(spendable_output(), Height(1), false),
                0,
            ),
        );
        transactions.push(Arc::new(transaction));
    }

    let block = Arc::new(Block {
        header: genesis.header,
        transactions,
    });

    if transaction_count == MAX_SPEND_TRANSACTIONS {
        assert!(
            block.zcash_serialized_size()
                <= usize::try_from(MAX_BLOCK_BYTES).expect("maximum block size fits in usize"),
            "maximum benchmark block must fit the consensus size limit"
        );

        let mut oversized_block = block.as_ref().clone();
        oversized_block
            .transactions
            .push(Arc::new(minimal_v5_spend(synthetic_outpoint(
                transaction_count,
            ))));
        assert!(
            oversized_block.zcash_serialized_size()
                > usize::try_from(MAX_BLOCK_BYTES).expect("maximum block size fits in usize"),
            "one more benchmark transaction must exceed the consensus size limit"
        );
    }

    let prepared = SemanticallyVerifiedBlock {
        block: block.clone(),
        hash: block.hash(),
        height: BENCHMARK_HEIGHT,
        new_outputs: HashMap::new(),
        transaction_hashes: vec![transaction::Hash([0; 32]); transaction_count + 1].into(),
        deferred_pool_balance_change: None,
        auth_data_root: None,
    };

    (block, prepared, spent_utxos)
}

fn minimal_v5_coinbase() -> Transaction {
    Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu5,
        lock_time: LockTime::unlocked(),
        expiry_height: BENCHMARK_HEIGHT,
        inputs: vec![transparent::Input::Coinbase {
            height: BENCHMARK_HEIGHT,
            data: Vec::new(),
            sequence: u32::MAX,
        }],
        outputs: vec![unspendable_output()],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    }
}

fn minimal_v5_spend(outpoint: transparent::OutPoint) -> Transaction {
    Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu5,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        inputs: vec![transparent::Input::PrevOut {
            outpoint,
            unlock_script: transparent::Script::new(&[]),
            sequence: u32::MAX,
        }],
        outputs: vec![unspendable_output()],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    }
}

fn synthetic_outpoint(index: usize) -> transparent::OutPoint {
    let unique = u64::try_from(index)
        .expect("block transaction count fits in u64")
        .checked_add(1)
        .expect("benchmark index is far below u64::MAX");
    let mut hash = [0; 32];
    hash[..8].copy_from_slice(&unique.to_le_bytes());

    transparent::OutPoint {
        hash: transaction::Hash(hash),
        index: 0,
    }
}

fn spendable_output() -> transparent::Output {
    transparent::Output::new(one_zatoshi(), transparent::Script::new(&[OP_TRUE]))
}

fn unspendable_output() -> transparent::Output {
    transparent::Output::new(one_zatoshi(), transparent::Script::new(&[]))
}

fn one_zatoshi() -> Amount<NonNegative> {
    1.try_into().expect("one zatoshi is a valid amount")
}

#[allow(clippy::print_stdout)]
fn print_result(transaction_count: usize, block_bytes: usize, elapsed: Duration) {
    let entries = transaction_count
        .checked_mul(transaction_count)
        .expect("benchmark entry count fits in usize");
    let transaction_count: u32 = transaction_count
        .try_into()
        .expect("benchmark transaction count fits in u32");
    let transactions_per_second = f64::from(transaction_count) / elapsed.as_secs_f64();

    println!(
        "transactions={transaction_count} block_bytes={block_bytes} \
         map_entries_per_phase={entries} elapsed_seconds={:.3} transactions_per_second={:.1}",
        elapsed.as_secs_f64(),
        transactions_per_second,
    );
}
