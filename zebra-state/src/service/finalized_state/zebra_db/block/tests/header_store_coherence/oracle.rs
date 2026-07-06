//! The in-memory oracle: a trivially correct model of the header store's
//! *specified* behavior.
//!
//! The model is a single linked canonical chain above genesis, plus a
//! sequential body tip. Chain selection is best-cumulative-work with strict
//! improvement, and an accepted branch switch replaces the whole suffix above
//! the first conflicting height — nothing of the losing branch survives.
//! Rejected commits change nothing.

use std::collections::{BTreeMap, HashMap};

use zebra_chain::{
    block::{self, Height},
    work::difficulty::PartialCumulativeWork,
};

use super::fabricate::{FabHeader, Universe};

/// What the oracle expects an op to do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Prediction {
    /// The store must accept the op.
    Accept,
    /// The store must reject the op and remain byte-identical.
    Reject(RejectKind),
    /// The op does not apply in the current state (unrealistic in production);
    /// the harness must not execute it.
    Skip(&'static str),
}

/// Why the oracle expects a rejection. Kinds are advisory — the runner only
/// cross-checks the accept/reject axis; scripted scenarios assert exact error
/// variants themselves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RejectKind {
    /// The anchor hash is not on the expected canonical chain.
    UnknownAnchor,
    /// The range conflicts below the finalized (body) tip.
    ImmutableConflict,
    /// The conflicting suffix does not carry strictly more work.
    LowerWork,
    /// The range is malformed (wrong anchor linkage or heights); the store is
    /// expected to reject it through contextual validation.
    Malformed,
}

/// A header range resolved against the universe: the anchor hash and the
/// fabricated rows to commit.
#[derive(Clone, Debug)]
pub(crate) struct ResolvedRange {
    pub anchor: block::Hash,
    pub rows: Vec<FabHeader>,
}

pub(crate) struct Oracle {
    /// The expected canonical chain above genesis.
    committed: BTreeMap<Height, block::Hash>,
    /// The sequential body/finalized tip. Bodies exist at `0..=body_tip`.
    body_tip: Height,
    /// Every fabricated header in the universe, by hash.
    index: HashMap<block::Hash, FabHeader>,
    genesis_hash: block::Hash,
}

impl Oracle {
    pub fn new(universe: &Universe) -> Self {
        let mut index = HashMap::new();
        for fab in universe
            .trunk
            .iter()
            .chain(universe.branches.iter().flat_map(|b| b.headers.iter()))
        {
            index.insert(fab.hash, fab.clone());
        }

        Oracle {
            committed: BTreeMap::new(),
            body_tip: Height(0),
            index,
            genesis_hash: universe.genesis.hash(),
        }
    }

    /// The expected canonical chain above genesis.
    pub fn canonical_chain(&self) -> &BTreeMap<Height, block::Hash> {
        &self.committed
    }

    pub fn body_tip(&self) -> Height {
        self.body_tip
    }

    /// The next height a body commit may target (bodies are sequential).
    pub fn next_body_height(&self) -> Height {
        Height(self.body_tip.0 + 1)
    }

    /// The fabricated header behind a canonical-chain hash.
    pub fn fab_for(&self, hash: block::Hash) -> Option<&FabHeader> {
        self.index.get(&hash)
    }

    fn anchor_height(&self, anchor: block::Hash) -> Option<Height> {
        if anchor == self.genesis_hash {
            return Some(Height(0));
        }
        self.committed
            .iter()
            .find(|(_, &hash)| hash == anchor)
            .map(|(&height, _)| height)
    }

    /// The first height in `rows` whose hash conflicts with the expected chain.
    fn first_conflict(&self, rows: &[FabHeader]) -> Option<Height> {
        rows.iter()
            .find(|row| {
                self.committed
                    .get(&row.height)
                    .is_some_and(|&existing| existing != row.hash)
            })
            .map(|row| row.height)
    }

    fn suffix_work_from(&self, from: Height) -> PartialCumulativeWork {
        let mut work = PartialCumulativeWork::zero();
        for (_height, hash) in self.committed.range(from..) {
            work += self
                .index
                .get(hash)
                .expect("committed hashes come from the universe")
                .work;
        }
        work
    }

    /// Predicts the outcome of a header-range commit.
    pub fn predict_header_range(&self, range: &ResolvedRange) -> Prediction {
        if range.rows.is_empty() {
            return Prediction::Skip("empty range");
        }

        let Some(anchor_height) = self.anchor_height(range.anchor) else {
            return Prediction::Reject(RejectKind::UnknownAnchor);
        };

        // A well-formed range links to its anchor and starts right above it.
        // Anything else must be rejected by the store's contextual validation.
        let first = &range.rows[0];
        if first.header.previous_block_hash != range.anchor
            || first.height != Height(anchor_height.0 + 1)
        {
            return Prediction::Reject(RejectKind::Malformed);
        }

        if let Some(first_conflict) = self.first_conflict(&range.rows) {
            if first_conflict <= self.body_tip {
                return Prediction::Reject(RejectKind::ImmutableConflict);
            }

            let existing_work = self.suffix_work_from(first_conflict);
            let mut new_work = PartialCumulativeWork::zero();
            for row in &range.rows {
                if row.height >= first_conflict {
                    new_work += row.work;
                }
            }
            if new_work <= existing_work {
                return Prediction::Reject(RejectKind::LowerWork);
            }
        }

        Prediction::Accept
    }

    /// Applies an accepted header-range commit to the model: total suffix
    /// replacement above the first conflicting height, then insert the rows.
    pub fn apply_header_range(&mut self, range: &ResolvedRange) {
        if let Some(first_conflict) = self.first_conflict(&range.rows) {
            self.committed.split_off(&first_conflict);
        }
        for row in &range.rows {
            self.committed.insert(row.height, row.hash);
        }
    }

    /// Predicts a body commit. Bodies are sequential in production, so only
    /// `body_tip + 1` is realistic; the block may belong to any branch (a body
    /// of the losing branch racing a header reorg).
    pub fn predict_body(&self, fab: &FabHeader) -> Prediction {
        if fab.height != self.next_body_height() {
            return Prediction::Skip("bodies commit sequentially at body_tip + 1");
        }
        Prediction::Accept
    }

    /// Applies a body commit: the verified body wins over provisional headers,
    /// truncating them when it conflicts.
    pub fn apply_body(&mut self, fab: &FabHeader) {
        if self.committed.get(&fab.height) != Some(&fab.hash) {
            self.committed.split_off(&fab.height);
        }
        self.committed.insert(fab.height, fab.hash);
        self.body_tip = fab.height;
    }

    /// The hash at `height` in `fab`'s fabricated ancestry (walking
    /// `previous_block_hash` links through the universe index).
    fn ancestor_hash_at(&self, fab: &FabHeader, height: Height) -> block::Hash {
        let mut current = fab.clone();
        while current.height.0 > height.0 + 1 {
            current = self
                .index
                .get(&current.header.previous_block_hash)
                .expect("fabricated ancestry stays inside the universe down to genesis")
                .clone();
        }
        current.header.previous_block_hash
    }

    /// Predicts a seed write (the non-finalized best-chain commit hook).
    /// The non-finalized state only
    /// holds blocks above the finalized tip, on chains rooted at it — a block
    /// whose ancestry does not pass through the finalized tip cannot be an
    /// nf best tip, so seeding it is unrealistic. Canonicality is decided by
    /// the non-finalized state, so the seed bypasses the header-store work
    /// gate. Note that an nf best-*tip* jump (a fork switch between nf chains,
    /// or an nf-backup restore) legitimately seeds a height whose parent row
    /// is missing or belongs to another branch.
    pub fn predict_seed(&self, fab: &FabHeader) -> Prediction {
        if fab.height <= self.body_tip {
            return Prediction::Skip("seeds only happen above the finalized tip");
        }

        let attach_point = self.ancestor_hash_at(fab, self.body_tip);
        let finalized_hash = if self.body_tip == Height(0) {
            self.genesis_hash
        } else {
            *self
                .committed
                .get(&self.body_tip)
                .expect("body heights stay on the expected chain")
        };
        if attach_point != finalized_hash {
            return Prediction::Skip("block is not attachable to the non-finalized state");
        }

        Prediction::Accept
    }

    /// Whether a seeded block's parent is the expected canonical row below it.
    /// Seeds that are not parent-linked are the known seed-path corruption
    /// shape (they write a row the store cannot link).
    pub fn seed_is_parent_linked(&self, fab: &FabHeader) -> bool {
        let parent_height = Height(fab.height.0 - 1);
        if parent_height == Height(0) {
            fab.header.previous_block_hash == self.genesis_hash
        } else {
            self.committed.get(&parent_height) == Some(&fab.header.previous_block_hash)
        }
    }

    /// Applies a seed write: the seeded block is the new best chain at its
    /// height, truncating any conflicting suffix.
    pub fn apply_seed(&mut self, fab: &FabHeader) {
        if self.committed.get(&fab.height) != Some(&fab.hash) {
            self.committed.split_off(&fab.height);
        }
        self.committed.insert(fab.height, fab.hash);
    }
}
