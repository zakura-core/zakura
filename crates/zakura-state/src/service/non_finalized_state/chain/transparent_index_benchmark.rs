//! Release-mode benchmark for the non-finalized transparent indexes under output-heavy blocks.
//!
//! The scenario mirrors a miner stuffing coinbases with distinct P2SH outputs while the
//! node holds several side chains and a published snapshot of every chain. It measures
//! pushing those blocks, finalizing them on every chain, and the address queries the
//! RPCs use, together with the heap each step retains or briefly needs.

use std::{collections::HashSet, hint::black_box, sync::Arc, time::Instant};

use zakura_chain::{
    amount::Amount,
    block::{Block, Height},
    parameters::Network,
    transaction::Transaction,
    transparent,
    value_balance::ValueBalance,
};

use crate::{ContextuallyVerifiedBlock, SemanticallyVerifiedBlock};

use super::Chain;

/// Reads the benchmark binary's global allocator counters.
#[derive(Clone, Copy)]
pub struct AllocationProbe {
    /// Bytes currently allocated.
    pub live_bytes: fn() -> usize,
    /// The highest `live_bytes` value since the last `reset_peak`.
    pub peak_live_bytes: fn() -> usize,
    /// Resets `peak_live_bytes` to the current `live_bytes`.
    pub reset_peak: fn(),
}

/// The shape of the benchmark's chains.
#[derive(Clone, Copy, Debug)]
pub struct TransparentIndexBenchmarkConfig {
    /// Blocks with stuffed coinbases on the best chain.
    pub stuffed_blocks: usize,
    /// P2SH outputs added to each stuffed coinbase.
    pub outputs_per_block: usize,
    /// The number of addresses the outputs cycle through, or zero for a new address per output.
    pub address_pool: usize,
    /// Side chains forked from the best chain, spread through the stuffed blocks.
    pub side_chains: usize,
    /// Root blocks finalized on every chain.
    pub finalizations: usize,
    /// Repetitions of each address query.
    pub query_samples: usize,
}

/// One measured step.
#[derive(Clone, Debug)]
pub struct TransparentIndexBenchmarkSample {
    /// The step that was measured.
    pub name: String,
    /// Wall-clock time for the step, or the median per query.
    pub nanos: u128,
    /// Heap retained after the step, relative to the start of the benchmark.
    pub retained_bytes: isize,
    /// Highest heap use during the step, relative to the heap before it.
    pub peak_extra_bytes: usize,
}

/// Runs the benchmark, using `template`'s coinbase for every block.
///
/// `template` must have a coinbase height. Its other transactions are dropped, so blocks
/// spend nothing.
pub fn benchmark_transparent_indexes(
    template: Arc<Block>,
    config: TransparentIndexBenchmarkConfig,
    probe: AllocationProbe,
) -> Vec<TransparentIndexBenchmarkSample> {
    let network = Network::Mainnet;
    let mut template = Block::clone(&template);
    template.transactions.truncate(1);
    let template = Arc::new(template);
    let first_height = template
        .coinbase_height()
        .expect("the template has a coinbase height");

    let mut samples = Vec::new();
    let baseline = (probe.live_bytes)();
    let retained = || {
        isize::try_from((probe.live_bytes)()).expect("heap fits in isize")
            - isize::try_from(baseline).expect("heap fits in isize")
    };

    let mut best = Chain::new(
        &network,
        (first_height - 1).expect("the template is above genesis"),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        ValueBalance::zero(),
    );
    // Every published state shares its chains with the writer until the next publish.
    let mut snapshot: Vec<Chain> = Vec::new();
    let mut side_chains: Vec<Chain> = Vec::new();
    let mut addresses = Vec::new();

    let fork_every = (config.stuffed_blocks / config.side_chains.max(1)).max(1);
    let mut parent = template.clone();
    let mut push_nanos = 0;
    (probe.reset_peak)();
    let before = (probe.live_bytes)();
    for index in 0..config.stuffed_blocks {
        let block = if index == 0 {
            stuffed(
                &parent,
                0,
                index,
                config.outputs_per_block,
                config.address_pool,
            )
        } else {
            stuffed(
                &child_of(&parent),
                0,
                index,
                config.outputs_per_block,
                config.address_pool,
            )
        };
        if index == 0 {
            addresses.extend(stuffed_addresses(&block, &network).take(100));
        }
        let start = Instant::now();
        best = best
            .push(contextual(&network, block.clone()))
            .expect("stuffed blocks are valid chain updates");
        push_nanos += start.elapsed().as_nanos();

        if side_chains.len() < config.side_chains && (index + 1) % fork_every == 0 {
            // A one-output sibling of `block`, so the side chain shares everything below it.
            let sibling = stuffed(&child_of(&parent), 1, index, 1, 0);
            let fork = best
                .fork(parent.hash())
                .or_else(|| (index == 0).then(|| best.clone()))
                .expect("the parent is in the best chain");
            if index > 0 {
                side_chains.push(
                    fork.push(contextual(&network, sibling))
                        .expect("siblings are valid chain updates"),
                );
            }
        }
        parent = block;
        snapshot = std::iter::once(best.clone())
            .chain(side_chains.iter().cloned())
            .collect();
    }
    samples.push(TransparentIndexBenchmarkSample {
        name: format!(
            "push {} blocks x {} outputs, {} chains",
            config.stuffed_blocks,
            config.outputs_per_block,
            side_chains.len() + 1
        ),
        nanos: push_nanos,
        retained_bytes: retained(),
        peak_extra_bytes: (probe.peak_live_bytes)().saturating_sub(before),
    });

    for finalization in 0..config.finalizations {
        (probe.reset_peak)();
        let before = (probe.live_bytes)();
        let start = Instant::now();
        best.pop_root();
        for side_chain in &mut side_chains {
            side_chain.pop_root();
        }
        let nanos = start.elapsed().as_nanos();
        let peak_extra_bytes = (probe.peak_live_bytes)().saturating_sub(before);
        // Publishing drops the previous snapshot.
        snapshot = std::iter::once(best.clone())
            .chain(side_chains.iter().cloned())
            .collect();
        samples.push(TransparentIndexBenchmarkSample {
            name: format!(
                "finalize root {} on {} chains",
                finalization + 1,
                side_chains.len() + 1
            ),
            nanos,
            retained_bytes: retained(),
            peak_extra_bytes,
        });
    }

    for count in [1, 100] {
        let queried: HashSet<_> = addresses.iter().take(count).cloned().collect();
        let range = Height(0)..=Height::MAX;
        let mut query = |name: &str, run: &dyn Fn()| {
            let mut timings: Vec<u128> = (0..config.query_samples)
                .map(|_| {
                    let start = Instant::now();
                    run();
                    start.elapsed().as_nanos()
                })
                .collect();
            timings.sort_unstable();
            samples.push(TransparentIndexBenchmarkSample {
                name: format!("{name} ({count} addresses)"),
                nanos: timings[timings.len() / 2],
                retained_bytes: retained(),
                peak_extra_bytes: 0,
            });
        };
        query("balance query", &|| {
            black_box(
                best.partial_transparent_balance_change(&queried, best.non_finalized_root_height()),
            );
        });
        query("utxo query", &|| {
            black_box(best.partial_transparent_utxo_changes(&queried));
        });
        query("tx id query", &|| {
            black_box(best.partial_transparent_tx_ids(&queried, range.clone()));
        });
    }

    black_box((snapshot, side_chains, best));
    samples
}

pub(super) fn contextual(network: &Network, block: Arc<Block>) -> ContextuallyVerifiedBlock {
    ContextuallyVerifiedBlock::with_block_and_spent_utxos(
        network,
        SemanticallyVerifiedBlock::from(block),
        Default::default(),
    )
    .expect("coinbase-only blocks have a valid value balance")
}

/// Returns a child of `parent` with the next coinbase height.
pub(super) fn child_of(parent: &Arc<Block>) -> Arc<Block> {
    let mut child = Block::clone(parent);
    let coinbase = Arc::make_mut(&mut child.transactions[0]);
    let input = match coinbase {
        Transaction::V4 { inputs, .. } | Transaction::V5 { inputs, .. } => &mut inputs[0],
        _ => panic!("the template coinbase is v4 or v5"),
    };
    match input {
        transparent::Input::Coinbase { height, .. } => height.0 += 1,
        _ => panic!("the first input is a coinbase input"),
    }
    Arc::make_mut(&mut child.header).previous_block_hash = parent.hash();
    Arc::new(child)
}

/// Replaces `block`'s extra coinbase outputs with `count` zero-value P2SH outputs.
///
/// With an `address_pool`, output `i` pays address `i % address_pool`. Otherwise `tag` and
/// `index` keep the script hashes distinct across blocks and siblings.
pub(super) fn stuffed(
    block: &Arc<Block>,
    tag: u8,
    index: usize,
    count: usize,
    address_pool: usize,
) -> Arc<Block> {
    let mut block = Block::clone(block);
    let coinbase = Arc::make_mut(&mut block.transactions[0]);
    let outputs = match coinbase {
        Transaction::V4 { outputs, .. } | Transaction::V5 { outputs, .. } => outputs,
        _ => panic!("the template coinbase is v4 or v5"),
    };
    outputs.truncate(1);
    outputs.extend((0..count).map(|output| {
        let (tag, index, output) = if address_pool > 0 {
            (0xaa, 0, output % address_pool)
        } else {
            (tag, index, output)
        };
        let mut script = vec![0xa9, 0x14];
        script.push(tag);
        script.extend_from_slice(&u32::try_from(index).expect("index fits").to_le_bytes());
        script.extend_from_slice(&u64::try_from(output).expect("output fits").to_le_bytes());
        script.extend_from_slice(&[0; 7]);
        script.push(0x87);
        transparent::Output {
            value: Amount::zero(),
            lock_script: transparent::Script::new(&script),
        }
    }));
    Arc::new(block)
}

fn stuffed_addresses<'a>(
    block: &'a Block,
    network: &'a Network,
) -> impl Iterator<Item = transparent::Address> + 'a {
    block.transactions[0]
        .outputs()
        .iter()
        .skip(1)
        .filter_map(|output| output.address(network))
}

/// Times the contextual spend check of a block whose inputs all spend outputs created by
/// earlier non-finalized blocks, the inputs that look up the chain's own indexes.
///
/// Builds a chain of `blocks` blocks with one transaction of `outputs_per_block` outputs
/// each, then checks a block with `spends` one-input transactions spending outputs spread
/// over the chain. Returns the median of `samples` checks.
pub fn benchmark_transparent_spend_check(
    template: Arc<Block>,
    blocks: usize,
    outputs_per_block: usize,
    spends: usize,
    samples: usize,
) -> u128 {
    use zakura_chain::transaction::LockTime;

    let network = Network::Mainnet;
    let mut template = Block::clone(&template);
    template.transactions.truncate(1);
    let template = Arc::new(template);
    let first_height = template
        .coinbase_height()
        .expect("the template has a coinbase height");
    let finalized =
        crate::service::finalized_state::FinalizedState::new(&crate::Config::ephemeral(), &network)
            .expect("an ephemeral database opens");

    let transaction = |inputs, outputs| {
        Arc::new(Transaction::V4 {
            inputs,
            outputs,
            lock_time: LockTime::unlocked(),
            expiry_height: Height(0),
            joinsplit_data: None,
            sapling_shielded_data: None,
        })
    };
    let output = |block: usize, index: usize| {
        let mut script = vec![0xa9, 0x14];
        script.extend_from_slice(&u32::try_from(block).expect("fits").to_le_bytes());
        script.extend_from_slice(&u64::try_from(index).expect("fits").to_le_bytes());
        script.extend_from_slice(&[0; 8]);
        script.push(0x87);
        transparent::Output {
            value: Amount::zero(),
            lock_script: transparent::Script::new(&script),
        }
    };

    let mut chain = Chain::new(
        &network,
        (first_height - 1).expect("the template is above genesis"),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        ValueBalance::zero(),
    );
    let mut parent = template.clone();
    let mut funding = Vec::with_capacity(blocks);
    for index in 0..blocks {
        let mut block = if index == 0 {
            Block::clone(&parent)
        } else {
            Block::clone(&child_of(&parent))
        };
        block.transactions.truncate(1);
        let funds = transaction(
            Vec::new(),
            (0..outputs_per_block).map(|i| output(index, i)).collect(),
        );
        funding.push(funds.hash());
        block.transactions.push(funds);
        let block = Arc::new(block);
        chain = chain
            .push(contextual(&network, block.clone()))
            .expect("funding blocks are valid chain updates");
        parent = block;
    }

    let mut candidate = Block::clone(&child_of(&parent));
    candidate.transactions.truncate(1);
    candidate.transactions.extend((0..spends).map(|spend| {
        let outpoint = transparent::OutPoint {
            hash: funding[spend % blocks],
            index: u32::try_from(spend / blocks % outputs_per_block).expect("fits"),
        };
        transaction(
            vec![transparent::Input::PrevOut {
                outpoint,
                unlock_script: transparent::Script::new(&[]),
                sequence: u32::MAX,
            }],
            Vec::new(),
        )
    }));
    let candidate = SemanticallyVerifiedBlock::from(Arc::new(candidate));

    let mut timings: Vec<u128> = (0..samples)
        .map(|_| {
            let start = Instant::now();
            let spent = black_box(spend_check(&candidate, &chain, &finalized.db));
            let nanos = start.elapsed().as_nanos();
            assert_eq!(spent.len(), spends, "every spend resolves in the chain");
            nanos
        })
        .collect();
    timings.sort_unstable();
    timings[timings.len() / 2]
}

fn spend_check(
    block: &SemanticallyVerifiedBlock,
    chain: &Chain,
    db: &crate::service::finalized_state::ZakuraDb,
) -> std::collections::HashMap<transparent::OutPoint, transparent::OrderedUtxo> {
    crate::service::check::utxo::transparent_spend(block, Some(chain), db)
        .expect("the candidate spends unspent outputs")
}
