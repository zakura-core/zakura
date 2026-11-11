//! Per-pass costs of the O(block bytes) encoding work a node does for one block.
//!
//! Builds full-size synthetic blocks from the real Sapling-era and NU5 mainnet
//! transactions in `zakura-test`, then times each pass a block can take between
//! the wire and RPC: decoding, encoding, sizing, hashing, and hex. Raw `memcpy`
//! and `sha256d` passes over the same bytes give a floor to compare against.
//!
//! Set `ZAKURA_BENCH_BLOCK_BYTES` to a comma-separated list of target sizes.
//! The default is `1000000,2000000`. Blocks above `MAX_BLOCK_BYTES` are decoded
//! as a header and a transaction list, which have no block-size limit, so the
//! per-pass costs of larger proposed limits can be measured directly.
//!
//! `ZAKURA_BENCH_BLOCK_MIX` picks how blocks are filled:
//!
//! - `post-nu7` (the default) builds blocks that fit the rules from NU7 on:
//!   only version 5 transactions (ZIP 2003), no JoinSplits, and shielded
//!   actions within every ZIP 218 per-block limit. The vectors' v5 shielded
//!   transactions are repeated until no more fit those limits, which uses
//!   nearly all of the shared shielded budget, and the rest of the block is
//!   the vectors' transparent-only transactions re-encoded as version 5.
//!   Scripts, proofs and signatures are the real mainnet bytes, but they do
//!   not verify against the new encoding. Repeated transactions spend renamed
//!   outpoints, so no transparent spend repeats, but shielded nullifiers do.
//! - `unbounded` is the pre-NU7 what-if: it cycles every vector transaction,
//!   v4 included, until the block is full, so shielded work grows with block
//!   size far past the ZIP 218 limits. No block like it can be mined from NU7.

// Disabled due to warnings in criterion macros
#![allow(missing_docs)]

use std::{hint::black_box, io::Cursor, sync::Arc};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use zakura_chain::{
    block::{merkle, Block, Header, MAX_BLOCK_BYTES},
    parameters::{
        NetworkUpgrade, GLOBAL_SHIELDED_BUDGET, ORCHARD_PROTOCOL_BLOCK_ACTION_LIMIT,
        SAPLING_BLOCK_IO_LIMIT, SPROUT_BLOCK_JOINSPLIT_LIMIT,
    },
    serialization::{sha256d, ZcashDeserialize, ZcashSerialize},
    transaction::{ShieldedActionCounts, Transaction, UnminedTx},
    transparent,
};

/// How a synthetic block is filled; see the module docs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BlockMix {
    PostNu7,
    Unbounded,
}

fn block_mix() -> BlockMix {
    match std::env::var("ZAKURA_BENCH_BLOCK_MIX").as_deref() {
        Err(_) | Ok("post-nu7") => BlockMix::PostNu7,
        Ok("unbounded") => BlockMix::Unbounded,
        Ok(other) => panic!("ZAKURA_BENCH_BLOCK_MIX is `post-nu7` or `unbounded`, not `{other}`"),
    }
}

/// Non-coinbase Sapling-era and NU5 mainnet transactions, in vector order.
fn vector_transactions() -> Vec<Arc<Transaction>> {
    zakura_test::vectors::MAINNET_BLOCKS
        .range(419_200..)
        .flat_map(|(_, bytes)| {
            Block::zcash_deserialize(Cursor::new(bytes))
                .expect("test vectors are valid blocks")
                .transactions
                .into_iter()
                .skip(1)
        })
        .collect()
}

/// A mainnet NU5 block cut down to its coinbase.
fn coinbase_only_block() -> Block {
    let mut block = Block::zcash_deserialize(Cursor::new(
        zakura_test::vectors::BLOCK_MAINNET_1687107_BYTES.as_slice(),
    ))
    .expect("test vectors are valid blocks");
    block.transactions.truncate(1);
    block
}

/// Re-encodes `tx` as a version 5 transaction for NU7, keeping its fields.
///
/// Returns `None` for a transaction version 5 cannot carry: one with
/// JoinSplits, or a v4 transaction with Sapling data, whose per-spend anchors
/// v5 does not have.
fn as_nu7_v5(tx: &Transaction) -> Option<Transaction> {
    match tx {
        Transaction::V4 {
            inputs,
            outputs,
            lock_time,
            expiry_height,
            joinsplit_data: None,
            sapling_shielded_data: None,
        } => Some(Transaction::V5 {
            network_upgrade: NetworkUpgrade::Nu7,
            lock_time: *lock_time,
            expiry_height: *expiry_height,
            inputs: inputs.clone(),
            outputs: outputs.clone(),
            sapling_shielded_data: None,
            orchard_shielded_data: None,
        }),
        Transaction::V5 {
            lock_time,
            expiry_height,
            inputs,
            outputs,
            sapling_shielded_data,
            orchard_shielded_data,
            ..
        } => Some(Transaction::V5 {
            network_upgrade: NetworkUpgrade::Nu7,
            lock_time: *lock_time,
            expiry_height: *expiry_height,
            inputs: inputs.clone(),
            outputs: outputs.clone(),
            sapling_shielded_data: sapling_shielded_data.clone(),
            orchard_shielded_data: orchard_shielded_data.clone(),
        }),
        _ => None,
    }
}

/// The version 5 `tx` with every spent outpoint renamed by `copy`, so each
/// copy of a transaction with transparent inputs has its own txid and spends.
fn renamed_copy(tx: &Transaction, copy: u32) -> Transaction {
    let mut tx = tx.clone();
    let Transaction::V5 { inputs, .. } = &mut tx else {
        panic!("post-NU7 transparent transactions are version 5");
    };
    for input in inputs {
        if let transparent::Input::PrevOut { outpoint, .. } = input {
            for (byte, copy_byte) in outpoint.hash.0.iter_mut().zip(copy.to_le_bytes()) {
                *byte ^= copy_byte;
            }
        }
    }
    tx
}

/// The ZIP 218 totals of every transaction in `block`.
fn shielded_totals(block: &Block) -> ShieldedActionCounts {
    block
        .transactions
        .iter()
        .map(|tx| tx.shielded_action_counts())
        .fold(
            ShieldedActionCounts::default(),
            ShieldedActionCounts::saturating_add,
        )
}

/// Whether `counts` fits every ZIP 218 per-block limit.
fn within_nu7_limits(counts: &ShieldedActionCounts) -> bool {
    let over_a_limit = counts.orchard_actions > ORCHARD_PROTOCOL_BLOCK_ACTION_LIMIT
        || counts.ironwood_actions > ORCHARD_PROTOCOL_BLOCK_ACTION_LIMIT
        || counts.sapling_ios > SAPLING_BLOCK_IO_LIMIT
        || counts.sprout_joinsplits > SPROUT_BLOCK_JOINSPLIT_LIMIT
        || counts.cost() > GLOBAL_SHIELDED_BUDGET;
    !over_a_limit
}

/// The vectors' transactions, split into what a post-NU7 block is built from.
struct PostNu7Pools {
    /// Version 5 transactions with shielded data, re-encoded for NU7.
    shielded: Vec<Arc<Transaction>>,
    /// Transparent-only transactions, re-encoded as version 5 for NU7.
    transparent: Vec<Transaction>,
}

impl PostNu7Pools {
    fn new(vector_transactions: &[Arc<Transaction>]) -> Self {
        let mut shielded = Vec::new();
        let mut transparent = Vec::new();
        for tx in vector_transactions {
            let Some(v5) = as_nu7_v5(tx) else { continue };
            if v5.shielded_action_counts() == ShieldedActionCounts::default() {
                transparent.push(v5);
            } else if tx.version() == 5 {
                shielded.push(Arc::new(v5));
            }
        }
        Self {
            shielded,
            transparent,
        }
    }

    /// A block of about `target_bytes`: a v5 coinbase, as many shielded
    /// transactions as the ZIP 218 limits allow, then transparent transactions.
    ///
    /// The shielded part stays the same size whatever `target_bytes` is, as it
    /// does in any post-NU7 block, once the block is large enough to hold it.
    fn block(&self, target_bytes: usize) -> Block {
        let mut block = coinbase_only_block();
        block.transactions[0] = Arc::new(
            as_nu7_v5(&block.transactions[0]).expect("the NU5 coinbase has no JoinSplits"),
        );

        let mut size = block.zcash_serialized_size();
        let mut counts = shielded_totals(&block);
        for copy in 0.. {
            let mut added = false;
            for tx in &self.shielded {
                let tx_size = tx.zcash_serialized_size();
                let with_tx = counts.saturating_add(tx.shielded_action_counts());
                if size + tx_size > target_bytes || !within_nu7_limits(&with_tx) {
                    continue;
                }
                size += tx_size;
                counts = with_tx;
                added = true;
                block.transactions.push(if copy == 0 {
                    tx.clone()
                } else {
                    Arc::new(renamed_copy(tx, copy))
                });
            }
            if !added {
                break;
            }
        }

        let copies = (0..).flat_map(|copy| self.transparent.iter().map(move |tx| (copy, tx)));
        for (copy, tx) in copies {
            let tx_size = tx.zcash_serialized_size();
            if size + tx_size > target_bytes {
                break;
            }
            size += tx_size;
            let tx = if copy == 0 {
                tx.clone()
            } else {
                renamed_copy(tx, copy)
            };
            block.transactions.push(Arc::new(tx));
        }

        assert_post_nu7_shape(&block);
        block
    }
}

/// Panics unless `block` has only v5 transactions and fits the ZIP 218 limits.
fn assert_post_nu7_shape(block: &Block) {
    assert!(
        block.transactions.iter().all(|tx| tx.version() == 5),
        "ZIP 2003 allows no transaction version below 5 from NU7"
    );
    let counts = shielded_totals(block);
    assert!(
        within_nu7_limits(&counts),
        "a post-NU7 block must fit the ZIP 218 limits, but it has {counts:?}, cost {}",
        counts.cost()
    );
}

/// A block of about `target_bytes` that cycles through `pool` after its coinbase.
fn unbounded_block(target_bytes: usize, pool: &[Arc<Transaction>]) -> Block {
    let mut block = coinbase_only_block();

    let mut size = block.zcash_serialized_size();
    for tx in pool.iter().cycle() {
        let tx_size = tx.zcash_serialized_size();
        if size + tx_size > target_bytes {
            break;
        }
        size += tx_size;
        block.transactions.push(tx.clone());
    }
    block
}

/// Names a block by its size, transaction count, ZIP 218 counts and
/// transparent shape, e.g. `1999KB/3786tx/o120-s210-c330/7904in-9113out`.
fn block_label(block: &Block, serialized_bytes: usize) -> String {
    let counts = shielded_totals(block);
    let inputs: usize = block.transactions.iter().map(|tx| tx.inputs().len()).sum();
    let outputs: usize = block.transactions.iter().map(|tx| tx.outputs().len()).sum();
    let ironwood = if counts.ironwood_actions == 0 {
        String::new()
    } else {
        format!("-i{}", counts.ironwood_actions)
    };
    let sprout = if counts.sprout_joinsplits == 0 {
        String::new()
    } else {
        format!("-j{}", counts.sprout_joinsplits)
    };
    format!(
        "{}KB/{}tx/o{}-s{}{ironwood}{sprout}-c{}/{inputs}in-{outputs}out",
        serialized_bytes / 1000,
        block.transactions.len(),
        counts.orchard_actions,
        counts.sapling_ios,
        counts.cost(),
    )
}

/// Decodes `bytes` as a block, without the consensus block-size limit.
fn decode_unbounded(mut bytes: &[u8]) -> Block {
    let header = Header::zcash_deserialize_from_slice(&mut bytes).expect("valid header");
    let transactions =
        Vec::<Arc<Transaction>>::zcash_deserialize_from_slice(&mut bytes).expect("valid list");
    assert!(bytes.is_empty(), "the block fills the bytes");
    Block {
        header: Arc::new(header),
        transactions,
    }
}

fn block_targets() -> Vec<usize> {
    std::env::var("ZAKURA_BENCH_BLOCK_BYTES")
        .unwrap_or_else(|_| "1000000,2000000".to_string())
        .split(',')
        .map(|size| size.trim().parse().expect("sizes are byte counts"))
        .collect()
}

fn block_encoding(c: &mut Criterion) {
    let mix = block_mix();
    let pool = vector_transactions();
    let post_nu7_pools = PostNu7Pools::new(&pool);

    for target in block_targets() {
        let block = match mix {
            BlockMix::PostNu7 => post_nu7_pools.block(target),
            BlockMix::Unbounded => unbounded_block(target, &pool),
        };
        let bytes = block.zcash_serialize_to_vec().expect("vec writes succeed");
        let hashes = block.transaction_hashes();
        let label = block_label(&block, bytes.len());

        let mut group = c.benchmark_group("block_encoding");
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.sample_size(20);

        group.bench_with_input(BenchmarkId::new("memcpy", &label), &bytes, |b, bytes| {
            b.iter(|| black_box(bytes.clone()))
        });
        group.bench_with_input(BenchmarkId::new("sha256d", &label), &bytes, |b, bytes| {
            b.iter(|| {
                let mut writer = sha256d::Writer::default();
                std::io::Write::write_all(&mut writer, bytes).expect("hash writes succeed");
                black_box(writer.finish())
            })
        });
        group.bench_with_input(
            BenchmarkId::new("hex_encode", &label),
            &bytes,
            |b, bytes| b.iter(|| black_box(hex::encode(bytes))),
        );
        if bytes.len() <= MAX_BLOCK_BYTES as usize {
            group.bench_with_input(
                BenchmarkId::new("deserialize_slice", &label),
                &bytes,
                |b, bytes| {
                    b.iter(|| {
                        let mut slice = bytes.as_slice();
                        black_box(Block::zcash_deserialize_from_slice(&mut slice).expect("valid"))
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("deserialize_cursor", &label),
                &bytes,
                |b, bytes| {
                    b.iter(|| {
                        black_box(Block::zcash_deserialize(Cursor::new(bytes)).expect("valid"))
                    })
                },
            );
        }
        group.bench_with_input(
            BenchmarkId::new("deserialize_unbounded", &label),
            &bytes,
            |b, bytes| b.iter(|| black_box(decode_unbounded(bytes))),
        );
        group.bench_with_input(BenchmarkId::new("serialize", &label), &block, |b, block| {
            b.iter(|| black_box(block.zcash_serialize_to_vec().expect("vec writes succeed")))
        });
        group.bench_with_input(
            BenchmarkId::new("serialized_size", &label),
            &block,
            |b, block| b.iter(|| black_box(block.zcash_serialized_size())),
        );
        group.bench_with_input(
            BenchmarkId::new("transaction_hashes", &label),
            &block,
            |b, block| b.iter(|| black_box(block.transaction_hashes())),
        );
        group.bench_with_input(
            BenchmarkId::new("txids_serial", &label),
            &block,
            |b, block| {
                b.iter(|| {
                    black_box(
                        block
                            .transactions
                            .iter()
                            .map(|tx| tx.hash())
                            .collect::<Vec<_>>(),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("auth_data_digests", &label),
            &block,
            |b, block| b.iter(|| black_box(block.auth_data_digests())),
        );
        group.bench_with_input(
            BenchmarkId::new("merkle_root", &label),
            &hashes,
            |b, hashes| b.iter(|| black_box(hashes.iter().copied().collect::<merkle::Root>())),
        );
        group.bench_with_input(
            BenchmarkId::new("block_hash", &label),
            &block,
            |b, block| b.iter(|| black_box(block.hash())),
        );
        group.bench_with_input(
            BenchmarkId::new("unmined_tx_from_each", &label),
            &block,
            |b, block| {
                b.iter(|| {
                    black_box(
                        block
                            .transactions
                            .iter()
                            .map(UnminedTx::from)
                            .collect::<Vec<_>>(),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("tx_serialize_each", &label),
            &block,
            |b, block| {
                b.iter(|| {
                    black_box(
                        block
                            .transactions
                            .iter()
                            .map(|tx| tx.zcash_serialize_to_vec().expect("vec writes succeed"))
                            .collect::<Vec<_>>(),
                    )
                })
            },
        );
        let spent_outputs: Vec<_> = block
            .transactions
            .iter()
            .skip(1)
            .map(|tx| {
                let nu = tx.network_upgrade().unwrap_or(NetworkUpgrade::Canopy);
                let outputs = Arc::new(vec![
                    transparent::Output {
                        value: 0.try_into().expect("zero is a valid amount"),
                        lock_script: transparent::Script::new(&[]),
                    };
                    tx.inputs().len()
                ]);
                tx.sighasher(nu, outputs.clone())
                    .expect("vector transactions convert to librustzcash");
                (tx.clone(), nu, outputs)
            })
            .collect();
        group.bench_with_input(
            BenchmarkId::new("sighasher_each", &label),
            &spent_outputs,
            |b, spent_outputs| {
                b.iter(|| {
                    for (tx, nu, outputs) in spent_outputs {
                        black_box(tx.sighasher(*nu, outputs.clone()).expect("converts"));
                    }
                })
            },
        );
        group.finish();
    }

    // Decode cost split by transaction version, to locate the expensive fields.
    // Only the unbounded mix has v4 transactions to split out.
    if mix != BlockMix::Unbounded {
        return;
    }
    let mut group = c.benchmark_group("block_decode_by_version");
    group.sample_size(20);
    for version in [4, 5] {
        let version_pool: Vec<_> = pool
            .iter()
            .filter(|tx| tx.version() == version)
            .cloned()
            .collect();
        let block = unbounded_block(2_000_000, &version_pool);
        let bytes = block.zcash_serialize_to_vec().expect("vec writes succeed");
        let label = format!(
            "v{version}/{}KB/{}tx",
            bytes.len() / 1000,
            block.transactions.len()
        );
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("deserialize_slice", &label),
            &bytes,
            |b, bytes| {
                b.iter(|| {
                    let mut slice = bytes.as_slice();
                    black_box(Block::zcash_deserialize_from_slice(&mut slice).expect("valid"))
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, block_encoding);
criterion_main!(benches);
