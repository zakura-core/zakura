//! Release-mode benchmark for the chain copies a node makes for each new block at the tip.
//!
//! The block writer shares every chain with the state it last published, so changing a
//! chain copies it first. For each new block a node at the tip copies the best chain to add
//! the block, publishes, copies every chain to remove the finalized root, and publishes
//! again, releasing the copies it replaced. This replays that cycle on chains as long as
//! the rollback window, with side chains that fork off the best chain.

use std::{hint::black_box, sync::Arc, time::Instant};

use zakura_chain::{block::Block, parameters::Network, value_balance::ValueBalance};

use super::{
    transparent_index_benchmark::{child_of, contextual, stuffed},
    Chain,
};

/// The shape of the benchmark's chains.
#[derive(Clone, Copy, Debug)]
pub struct ChainSnapshotBenchmarkConfig {
    /// Blocks on the best chain, the length the rollback window keeps it at.
    pub blocks: usize,
    /// Transparent outputs added to each block's coinbase.
    pub outputs_per_block: usize,
    /// Side chains forked from the best chain, spread along it.
    pub side_chains: usize,
    /// New blocks committed and finalized after the chains are built.
    pub cycles: usize,
    /// Bytes written between cycles to push the chains out of the CPU caches, as the time
    /// between blocks does on a node. Zero leaves the caches warm.
    pub cache_eviction_bytes: usize,
}

/// The median time of one step of the per-block cycle.
#[derive(Clone, Debug)]
pub struct ChainSnapshotBenchmarkSample {
    /// The step that was measured.
    pub name: &'static str,
    /// The median nanoseconds the step took per new block.
    pub median_nanos: u128,
    /// The mean nanoseconds the step took per new block.
    pub mean_nanos: u128,
}

/// Runs the benchmark, using `template`'s coinbase for every block.
///
/// `template` must have a coinbase height. Its other transactions are dropped.
pub fn benchmark_chain_snapshot_cycle(
    template: Arc<Block>,
    config: ChainSnapshotBenchmarkConfig,
) -> Vec<ChainSnapshotBenchmarkSample> {
    let network = Network::Mainnet;
    let mut template = Block::clone(&template);
    template.transactions.truncate(1);
    let template = Arc::new(template);
    let first_height = template
        .coinbase_height()
        .expect("the template has a coinbase height");

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
    let mut side_chains: Vec<Arc<Chain>> = Vec::new();

    let fork_every = (config.blocks / config.side_chains.max(1)).max(1);
    let mut parent = template.clone();
    for index in 0..config.blocks {
        let block = if index == 0 {
            stuffed(&parent, 0, index, config.outputs_per_block, 0)
        } else {
            stuffed(&child_of(&parent), 0, index, config.outputs_per_block, 0)
        };
        if index > 0 && side_chains.len() < config.side_chains && (index + 1) % fork_every == 0 {
            // A sibling of `block`, so the side chain shares everything below it.
            let sibling = stuffed(&child_of(&parent), 1, index, 1, 0);
            let fork = best
                .fork(parent.hash())
                .expect("the parent is in the best chain");
            side_chains.push(Arc::new(
                fork.push(contextual(&network, sibling))
                    .expect("siblings are valid chain updates"),
            ));
        }
        best = best
            .push(contextual(&network, block.clone()))
            .expect("stuffed blocks are valid chain updates");
        parent = block;
    }
    let mut best = Arc::new(best);

    // The published state holds every chain the writer holds.
    let publish = |best: &Arc<Chain>, side_chains: &[Arc<Chain>]| -> Vec<Arc<Chain>> {
        std::iter::once(best)
            .chain(side_chains)
            .map(Arc::clone)
            .collect()
    };
    let mut published = publish(&best, &side_chains);

    let mut eviction = vec![0u8; config.cache_eviction_bytes];
    let mut timings: [Vec<u128>; 6] = Default::default();
    for cycle in 0..config.cycles {
        for (index, byte) in eviction.iter_mut().enumerate() {
            // Truncation is intended: any changing value dirties the line.
            *byte = (index + cycle) as u8;
        }
        black_box(&eviction);

        let block = stuffed(
            &child_of(&parent),
            0,
            config.blocks + cycle,
            config.outputs_per_block,
            0,
        );
        let block_for_chain = contextual(&network, block.clone());
        let grandparent = std::mem::replace(&mut parent, block);

        // Commit: the published state shares the best chain, so adding a block copies it.
        let start = Instant::now();
        let copy = Chain::clone(&best);
        let commit_copy = start.elapsed().as_nanos();

        let start = Instant::now();
        let pushed = Arc::new(
            copy.push(block_for_chain)
                .expect("stuffed blocks are valid chain updates"),
        );
        let commit_push = start.elapsed().as_nanos();
        best = pushed;

        // Publishing releases the chains of the state it replaces.
        let start = Instant::now();
        drop(std::mem::replace(
            &mut published,
            publish(&best, &side_chains),
        ));
        let publish_commit = start.elapsed().as_nanos();

        // A side chain ends when the finalized root reaches its fork point. A new fork at
        // the tip replaces it, so the chains keep the spread of lengths a node has.
        let before = side_chains.len();
        side_chains.retain(|side_chain| side_chain.len() > 1);
        for _ in side_chains.len()..before {
            let sibling = stuffed(&child_of(&grandparent), 1, config.blocks + cycle, 1, 0);
            let fork = best
                .fork(grandparent.hash())
                .expect("the tip's parent is in the best chain");
            side_chains.push(Arc::new(
                fork.push(contextual(&network, sibling))
                    .expect("siblings are valid chain updates"),
            ));
        }
        drop(std::mem::replace(
            &mut published,
            publish(&best, &side_chains),
        ));

        // Finalize: every chain drops the same root, and every chain is shared.
        let start = Instant::now();
        Arc::make_mut(&mut best).pop_root();
        for side_chain in &mut side_chains {
            Arc::make_mut(side_chain).pop_root();
        }
        let finalize = start.elapsed().as_nanos();

        let start = Instant::now();
        drop(std::mem::replace(
            &mut published,
            publish(&best, &side_chains),
        ));
        let publish_finalize = start.elapsed().as_nanos();

        let steps = [
            commit_copy,
            commit_push,
            publish_commit,
            finalize,
            publish_finalize,
        ];
        for (step, nanos) in steps.into_iter().enumerate() {
            timings[step].push(nanos);
        }
        timings[5].push(steps.into_iter().sum());
    }
    black_box((published, side_chains, best));

    let names = [
        "copy the best chain to add a block",
        "add the block",
        "publish the new tip",
        "copy every chain and drop its root",
        "publish the finalized state",
        "whole cycle",
    ];
    names
        .into_iter()
        .zip(timings)
        .map(|(name, mut nanos)| {
            nanos.sort_unstable();
            let count = u128::try_from(nanos.len().max(1)).expect("a length fits in u128");
            ChainSnapshotBenchmarkSample {
                name,
                median_nanos: nanos.get(nanos.len() / 2).copied().unwrap_or(0),
                mean_nanos: nanos.iter().sum::<u128>() / count,
            }
        })
        .collect()
}
