//! How many recent blocks named each gateway.

use std::{
    collections::{BTreeMap, HashMap},
    ops::RangeInclusive,
};

use crate::{
    block::{self, Height},
    parameters::Network,
};

use super::{window::window, MinerMeshCommitment};

/// The gateways named by the best chain's recent blocks, with a count each.
///
/// The set looks at a window of heights that follows the tip: two days' worth
/// of blocks ending 100 blocks below it. Its owner tells it where the tip is
/// with [`set_tip`](Self::set_tip) and reads blocks into it, in any order,
/// with [`record`](Self::record). A height counts once, however often it is
/// read.
///
/// The set does not watch the chain itself. After a reorg its owner compares
/// [`checked_blocks`](Self::checked_blocks) with the best chain and removes
/// what was displaced with [`forget_above`](Self::forget_above). Blocks from
/// the old and the new chain can be in the set together, so a block that is
/// still on the best chain says nothing about the blocks below it: the owner
/// compares every height the reorg could have reached.
#[derive(Clone, Debug)]
pub struct RecentMinerSet {
    network: Network,
    tip: Option<(Height, block::Hash)>,
    /// The heights that count at the current tip.
    window: Option<RangeInclusive<Height>>,
    /// The blocks read so far, all inside `window`.
    checked: BTreeMap<Height, CheckedBlock>,
}

/// One block the set has read.
#[derive(Copy, Clone, Debug)]
struct CheckedBlock {
    hash: block::Hash,
    gateway_id: Option<[u8; 32]>,
}

/// A gateway and the number of counted blocks that name it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct GatewayBlocks {
    /// The gateway ID exactly as the blocks wrote it.
    pub gateway_id: [u8; 32],
    /// How many counted blocks name this gateway. Never zero.
    pub blocks: u32,
}

/// The counts at one moment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MinerSetSnapshot {
    /// The best chain's tip when the counts were taken, if the set has one.
    pub tip: Option<(Height, block::Hash)>,
    /// Every gateway named in the checked blocks: most blocks first, and
    /// lower ID bytes first among equal counts.
    pub gateways: Vec<GatewayBlocks>,
    /// How many heights in the window have been read.
    pub checked: usize,
    /// The heights that count at the tip, oldest to newest, or `None` before
    /// the first tip and while the tip is below height 100.
    pub window: Option<RangeInclusive<Height>>,
}

/// How much of its window a [`RecentMinerSet`] has read, without the
/// counts: a small summary to show an operator.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MinerSetCoverage {
    /// The best chain's tip height when the summary was taken, if the set
    /// has one.
    pub tip: Option<Height>,
    /// The heights that count at the tip, as in [`MinerSetSnapshot::window`].
    pub window: Option<RangeInclusive<Height>>,
    /// How many heights in the window have been read.
    pub checked: usize,
}

impl MinerSetSnapshot {
    /// Returns whether the set has a tip and every height in its window has
    /// been read.
    ///
    /// Until then the counts are a lower bound, and an empty list only means
    /// that no gateway has been found yet.
    pub fn is_complete(&self) -> bool {
        self.tip.is_some() && self.checked == self.window_len()
    }

    /// How many heights the window holds.
    pub fn window_len(&self) -> usize {
        window_len(&self.window)
    }

    /// The tip, the window and how much of it is read, without the counts.
    pub fn coverage(&self) -> MinerSetCoverage {
        MinerSetCoverage {
            tip: self.tip.map(|(height, _)| height),
            window: self.window.clone(),
            checked: self.checked,
        }
    }
}

impl MinerSetCoverage {
    /// How many heights the window holds.
    pub fn window_len(&self) -> usize {
        window_len(&self.window)
    }
}

impl RecentMinerSet {
    /// Returns an empty set for `network`, with no tip yet.
    pub fn new(network: Network) -> Self {
        Self {
            network,
            tip: None,
            window: None,
            checked: BTreeMap::new(),
        }
    }

    /// Moves the window to follow a best chain whose tip is `height`.
    ///
    /// Blocks that are now too old stop counting. So do blocks that are now
    /// less than 100 below the tip, which happens when the tip moves back.
    /// `hash` is only reported back in [`snapshot`](Self::snapshot).
    pub fn set_tip(&mut self, height: Height, hash: block::Hash) {
        self.tip = Some((height, hash));
        self.window = window(&self.network, height);

        let window = &self.window;
        self.checked
            .retain(|checked, _| window.as_ref().is_some_and(|w| w.contains(checked)));
    }

    /// Reads the best-chain block at `height` into the set.
    ///
    /// `commitment` is the block's coinbase commitment, if it has exactly one.
    /// A height outside the window is ignored. A height that was already read
    /// is replaced, so reading the same block again changes nothing.
    pub fn record(
        &mut self,
        height: Height,
        hash: block::Hash,
        commitment: Option<MinerMeshCommitment>,
    ) {
        if !self.counts_at(height) {
            return;
        }

        let gateway_id = commitment.map(|commitment| commitment.gateway_id());
        self.checked
            .insert(height, CheckedBlock { hash, gateway_id });
    }

    /// Removes every block above `height`: they stop counting, and their
    /// heights are unchecked again.
    pub fn forget_above(&mut self, height: Height) {
        self.checked.retain(|checked, _| *checked <= height);
    }

    /// Returns the height and hash of every block the set holds, newest
    /// first.
    pub fn checked_blocks(&self) -> impl Iterator<Item = (Height, block::Hash)> + '_ {
        self.checked
            .iter()
            .rev()
            .map(|(height, block)| (*height, block.hash))
    }

    /// Returns the heights in the window that have not been read, newest
    /// first.
    pub fn unchecked_heights(&self) -> impl Iterator<Item = Height> + '_ {
        self.window
            .iter()
            .flat_map(|window| heights(window).rev())
            .map(Height)
            .filter(|height| !self.checked.contains_key(height))
    }

    /// Returns the tip, the counts and the coverage right now.
    ///
    /// The counts are those of the blocks the set holds. After a reorg they
    /// include displaced blocks until the owner has removed them, so the
    /// owner repairs the set before it takes a snapshot.
    pub fn snapshot(&self) -> MinerSetSnapshot {
        let mut blocks_by_gateway: HashMap<[u8; 32], u32> = HashMap::new();
        for gateway_id in self.checked.values().filter_map(|block| block.gateway_id) {
            *blocks_by_gateway.entry(gateway_id).or_default() += 1;
        }

        let mut gateways: Vec<GatewayBlocks> = blocks_by_gateway
            .into_iter()
            .map(|(gateway_id, blocks)| GatewayBlocks { gateway_id, blocks })
            .collect();
        gateways.sort_unstable_by(|a, b| {
            b.blocks
                .cmp(&a.blocks)
                .then_with(|| a.gateway_id.cmp(&b.gateway_id))
        });

        MinerSetSnapshot {
            tip: self.tip,
            gateways,
            checked: self.checked.len(),
            window: self.window.clone(),
        }
    }

    /// Returns whether a block at `height` counts at the current tip.
    fn counts_at(&self, height: Height) -> bool {
        self.window
            .as_ref()
            .is_some_and(|window| window.contains(&height))
    }
}

/// Returns how many heights `window` holds: none when there is no window.
fn window_len(window: &Option<RangeInclusive<Height>>) -> usize {
    window.as_ref().map_or(0, |window| heights(window).count())
}

/// Returns the heights in `window`, oldest first.
fn heights(window: &RangeInclusive<Height>) -> RangeInclusive<u32> {
    window.start().0..=window.end().0
}
