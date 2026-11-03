//! Contextual validation results from recent block proposals, reused when the solved block commits.
//!
//! A mining pool validates each block template as a proposal before handing it out, then
//! submits the solved block. Both run the same contextual validation against the same parent
//! with the same transactions, so the commit can reuse what the proposal already built instead
//! of building it a second time.
//!
//! Two things carry over. A proposal's transparent spend check result is reused whenever the
//! solved block has the same parent and transaction IDs. The whole chain a proposal built is
//! reused as well, but only while the parent chain it extended has not changed at all, so that
//! the solved block needs no more than the records that read its header.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use zakura_chain::{block, transaction, transparent};

use super::Chain;
use crate::{ContextuallyVerifiedBlock, SemanticallyVerifiedBlock};

#[cfg(test)]
mod tests;

/// How many proposal results to keep. Templates are refreshed one at a time, so only the
/// latest few can still be solved.
const MAX_ENTRIES: usize = 4;

/// How many of those results also keep the chain the proposal built.
///
/// A retained chain costs more memory than a spend result: it holds its own copy of the parts of
/// the chain that are not shared between snapshots, and it keeps the partitions the proposal's
/// update copied. It also holds the parent chain it extended, which stays allocated after the
/// tip moves on until the entry drops it. Only the newest templates can realistically be solved,
/// so older entries keep their spend result and drop their chain.
///
/// On a 100-block chain of 165-transaction blocks, `chain_clone_worst_case` puts an unshared
/// clone of one chain index at about 3 MB and a shared one at under a kilobyte, and the parts a
/// chain does copy outright grow with its block count rather than its transaction count. Confirm
/// the figure against a mining node's resident set before raising this.
const MAX_PREPARED_CHAINS: usize = 2;

/// The chain a validated proposal built, with the parent chain it was built on.
pub(crate) struct PreparedProposal {
    /// The chain the proposal extended, held so that it cannot change unnoticed.
    ///
    /// A non-finalized state keeps its chains in a set of `Arc<Chain>`, which lends out no
    /// mutable reference to a chain, so every change to a chain in that set replaces the
    /// chain's `Arc`. Holding this strong reference keeps the strong count above one, so
    /// `Arc::make_mut` and `Arc::unwrap_or_clone` have to copy rather than change the chain in
    /// place. Pointer equality therefore proves that the parent chain is the one this proposal
    /// extended, with the same blocks, the same finalized boundary, and nothing invalidated
    /// since.
    parent_chain: Arc<Chain>,

    /// The chain the proposal's contextual validation produced, one block longer.
    child_chain: Arc<Chain>,
}

/// Why a solved block cannot be finished from a retained proposal chain.
///
/// Each of these costs the commit the reuse and nothing else: it runs the ordinary contextual
/// validation instead.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NotPrepared {
    /// No validated proposal with this parent and these transaction IDs is retained.
    NoResult,

    /// The block's parent is the finalized tip, so its chain is created afresh rather than
    /// extended from one in the non-finalized state, and no proposal chain can be matched to it
    /// by identity.
    NewChain,

    /// A proposal's result is retained, but its chain was dropped to stay inside the memory
    /// bound.
    ChainNotRetained,

    /// The parent chain changed after the proposal: it grew, finalized, reorganized, or had a
    /// block invalidated.
    ParentChanged,

    /// The two blocks agree on every transaction ID but not on the authorizing data of some
    /// transaction, so the proposal's verification does not cover this body.
    TransactionsDiffer,

    /// The solved header claims a different difficulty threshold than the proposal, so the
    /// chain's cumulative work would have to change.
    DifficultyThresholdDiffers,
}

impl NotPrepared {
    /// The metric label for this reason.
    pub(crate) fn metric_label(self) -> &'static str {
        match self {
            Self::NoResult => "no_result",
            Self::NewChain => "new_chain",
            Self::ChainNotRetained => "chain_not_retained",
            Self::ParentChanged => "parent_changed",
            Self::TransactionsDiffer => "transactions_differ",
            Self::DifficultyThresholdDiffers => "difficulty_threshold_differs",
        }
    }
}

impl PreparedProposal {
    /// A proposal's `child_chain`, built by extending `parent_chain`.
    pub(crate) fn new(parent_chain: &Arc<Chain>, child_chain: &Arc<Chain>) -> Self {
        Self {
            parent_chain: Arc::clone(parent_chain),
            child_chain: Arc::clone(child_chain),
        }
    }

    /// Returns the chain to finish `solved` from, or why this one cannot be reused for it.
    ///
    /// `parent_chain` is the chain the commit resolved for `solved`'s parent. Everything the
    /// returned chain holds was derived from that parent and from transactions equal to
    /// `solved`'s, so only the records that read the block header are left to rebuild.
    fn for_solved_block(
        self,
        solved: &SemanticallyVerifiedBlock,
        parent_chain: &Arc<Chain>,
    ) -> Result<Arc<Chain>, NotPrepared> {
        if !Arc::ptr_eq(&self.parent_chain, parent_chain) {
            return Err(NotPrepared::ParentChanged);
        }

        let proposal = self
            .child_chain
            .tip_block()
            .expect("a proposal chain holds the block its validation pushed");

        // The transaction IDs already match, so this compares the authorizing data as well.
        if proposal.block.transactions != solved.block.transactions {
            return Err(NotPrepared::TransactionsDiffer);
        }

        if proposal.block.header.difficulty_threshold != solved.block.header.difficulty_threshold {
            return Err(NotPrepared::DifficultyThresholdDiffers);
        }

        Ok(self.child_chain)
    }
}

/// One proposal's results.
struct Entry {
    parent_hash: block::Hash,
    transaction_hashes: Arc<[transaction::Hash]>,
    spent_utxos: Arc<HashMap<transparent::OutPoint, transparent::OrderedUtxo>>,
    prepared_proposal: Option<PreparedProposal>,
}

/// Contextual validation results shared by a non-finalized state and its clones.
///
/// These results read only the parent chain and the transactions' effecting data: which outputs
/// they spend and what they create and pay. A block's parent hash fixes the parent chain, and
/// its transaction IDs commit to the effecting data of every transaction, so a block with the
/// same parent and the same transaction ID list gets the same result.
#[derive(Clone, Default)]
pub(crate) struct ProposalResults(Arc<Mutex<VecDeque<Entry>>>);

impl ProposalResults {
    /// Shares a validated proposal's results without copying them.
    ///
    /// `chain`, when given, must be the chain whose tip is `proposal`. Call only after all
    /// contextual checks and the proposal's chain update succeed.
    pub(crate) fn remember(
        &self,
        proposal: &ContextuallyVerifiedBlock,
        chain: Option<PreparedProposal>,
    ) {
        if let Some(chain) = &chain {
            assert_eq!(
                chain.child_chain.non_finalized_tip_hash(),
                proposal.hash,
                "a retained chain's tip is the proposal whose results it holds",
            );
        }

        let entry = Entry {
            parent_hash: proposal.block.header.previous_block_hash,
            transaction_hashes: proposal.transaction_hashes.clone(),
            spent_utxos: Arc::clone(&proposal.spent_outputs),
            prepared_proposal: chain,
        };
        let mut entries = self.lock();
        entries.retain(|old| !old.matches(entry.parent_hash, &entry.transaction_hashes));
        if entries.len() == MAX_ENTRIES {
            entries.pop_front();
        }
        entries.push_back(entry);
        bound_prepared_chains(&mut entries);
    }

    /// Takes what a validated proposal for `solved` left behind: the outputs its transparent
    /// spend check found, and the chain it built or the reason that chain cannot be reused.
    ///
    /// `parent_chain` is the chain the commit resolved for `solved`'s parent, or `None` when the
    /// commit is creating the chain itself, which no proposal chain can be matched to by
    /// identity.
    ///
    /// Both halves are independent: a chain this cannot offer still leaves the spend result,
    /// which the ordinary contextual validation uses in place of looking every spent output up
    /// again.
    pub(crate) fn take_for(
        &self,
        solved: &SemanticallyVerifiedBlock,
        parent_chain: Option<&Arc<Chain>>,
    ) -> (
        Option<Arc<HashMap<transparent::OutPoint, transparent::OrderedUtxo>>>,
        Result<Arc<Chain>, NotPrepared>,
    ) {
        let Some(entry) = self.take(solved) else {
            return (None, Err(NotPrepared::NoResult));
        };

        let reuse = match (parent_chain, entry.prepared_proposal) {
            (Some(parent_chain), Some(proposal)) => proposal.for_solved_block(solved, parent_chain),
            (Some(_), None) => Err(NotPrepared::ChainNotRetained),
            (None, _) => Err(NotPrepared::NewChain),
        };

        (Some(entry.spent_utxos), reuse)
    }

    /// Removes and returns the entry for a proposal with `block`'s parent and transactions, if
    /// one was validated.
    fn take(&self, block: &SemanticallyVerifiedBlock) -> Option<Entry> {
        let mut entries = self.lock();
        let index = entries.iter().position(|entry| {
            entry.matches(
                block.block.header.previous_block_hash,
                &block.transaction_hashes,
            )
        })?;
        entries.remove(index)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<Entry>> {
        // The lock is only held for simple list operations that cannot panic midway.
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Drops the chains of all but the newest [`MAX_PREPARED_CHAINS`] entries, keeping their spend
/// results.
fn bound_prepared_chains(entries: &mut VecDeque<Entry>) {
    let past_the_bound = entries.len().saturating_sub(MAX_PREPARED_CHAINS);
    for entry in entries.iter_mut().take(past_the_bound) {
        entry.prepared_proposal = None;
    }
}

impl Entry {
    fn matches(&self, parent_hash: block::Hash, transaction_hashes: &[transaction::Hash]) -> bool {
        self.parent_hash == parent_hash && self.transaction_hashes.as_ref() == transaction_hashes
    }
}
