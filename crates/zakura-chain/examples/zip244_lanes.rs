//! Prototype benchmark: per-transaction ZIP-244 hashing (#2022) versus the SIMD lane path in
//! `BlockRef::transaction_digests`, over blocks built from mainnet test-vector transactions.

#![allow(clippy::print_stdout, clippy::print_stderr)]

use rayon::prelude::*;
use std::{hint::black_box, sync::Arc, time::Instant};
use zakura_chain::{
    block::{merkle::AuthDataRoot, Block},
    serialization::ZcashDeserialize,
    transaction::{AuthDigest, Transaction},
};

/// #2022's `transaction_digests`: one scalar ZIP-244 walk per transaction.
fn per_transaction(block: &Block) -> AuthDataRoot {
    let digests: Vec<_> = if block.transactions.len() < 64 || rayon::current_num_threads() == 1 {
        block
            .transactions
            .iter()
            .map(|tx| tx.txid_and_auth_digest())
            .collect()
    } else {
        block
            .transactions
            .par_iter()
            .map(|tx| tx.txid_and_auth_digest())
            .collect()
    };
    digests
        .into_iter()
        .map(|(_, auth)| auth.unwrap_or(AuthDigest([0xff; 32])))
        .collect()
}

fn time(f: impl Fn() -> AuthDataRoot, iters: usize) -> f64 {
    let start = Instant::now();
    for _ in 0..iters {
        black_box(f());
    }
    start.elapsed().as_secs_f64() * 1e6 / iters as f64
}

fn main() {
    let (mut orchard, mut sapling, mut transparent) = (vec![], vec![], vec![]);
    for bytes in zakura_test::vectors::BLOCKS.iter() {
        let block = Block::zcash_deserialize(&bytes[..]).unwrap();
        for tx in block
            .transactions
            .iter()
            .filter(|tx| tx.version() == 5 && !tx.is_coinbase())
        {
            if tx.has_orchard_shielded_data() {
                orchard.push(tx.clone());
            } else if tx.has_sapling_shielded_data() {
                sapling.push(tx.clone());
            } else {
                transparent.push(tx.clone());
            }
        }
    }
    let actions: Vec<_> = orchard
        .iter()
        .map(|tx: &Arc<Transaction>| tx.orchard_actions().count())
        .collect();
    eprintln!(
        "lanes degree={} threads={} orchard actions per tx={actions:?}",
        blake2b_simd::many::degree(),
        rayon::current_num_threads()
    );
    let template = Block::zcash_deserialize(zakura_test::vectors::BLOCKS[0]).unwrap();

    let mut mixed = Vec::new();
    for (i, tx) in transparent
        .iter()
        .chain(&sapling)
        .cycle()
        .take(200)
        .enumerate()
    {
        mixed.push(tx.clone());
        if i % 20 == 0 {
            mixed.push(orchard[i / 20 % orchard.len()].clone());
        }
    }

    // Each variant is timed against the per-transaction baseline in alternating rounds.
    type Variant<'a> = (&'a str, &'a dyn Fn(&Block) -> AuthDataRoot);
    let lanes = |block: &Block| block.transaction_digests().auth_data_root;
    let variants: [Variant; 2] = [("A/A baseline", &per_transaction), ("lanes", &lanes)];
    println!("corpus,count,variant,baseline_us,variant_us,ratio");
    for (name, corpus) in [
        ("v5_orchard", &orchard),
        ("v5_sapling", &sapling),
        ("v5_transparent", &transparent),
        ("mixed_1_in_20_orchard", &mixed),
    ] {
        for count in [100, 220, 1000] {
            let mut block = template.clone();
            block.transactions = corpus.iter().cycle().take(count).cloned().collect();
            for (variant, f) in variants {
                assert_eq!(per_transaction(&block), f(&block));
                let iters = (100_000 / count).max(5);
                let (mut a, mut b) = (vec![], vec![]);
                for round in 0..21 {
                    let first = round % 2 == 0;
                    for which in [first, !first] {
                        if which {
                            a.push(time(|| per_transaction(&block), iters));
                        } else {
                            b.push(time(|| f(&block), iters));
                        }
                    }
                }
                a.sort_by(f64::total_cmp);
                b.sort_by(f64::total_cmp);
                let (a, b) = (a[10], b[10]);
                println!("{name},{count},{variant},{a:.1},{b:.1},{:.2}", a / b);
            }
        }
    }
}
