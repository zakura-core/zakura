//! Transparent spend results from recent block proposals, reused when the solved block commits.
//!
//! A mining pool validates each block template as a proposal before handing it out, then
//! submits the solved block. Both run the same transparent spend check against the same
//! parent, so the commit can reuse the proposal's result instead of looking up every spent
//! output again.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use zakura_chain::{block, transaction, transparent};

use crate::SemanticallyVerifiedBlock;

/// How many proposal results to keep. Templates are refreshed one at a time, so only the
/// latest few can still be solved.
const MAX_ENTRIES: usize = 4;

/// The spent outputs of one validated proposal.
struct Entry {
    parent_hash: block::Hash,
    transaction_hashes: Arc<[transaction::Hash]>,
    spent_utxos: HashMap<transparent::OutPoint, transparent::OrderedUtxo>,
}

/// Spend check results shared by a non-finalized state and its clones.
///
/// The spend check reads only the parent chain and the transactions' effecting data: which
/// outputs they spend and what they create and pay. A block's parent hash fixes the parent
/// chain, and its transaction IDs commit to the effecting data of every transaction, so a
/// block with the same parent and the same transaction ID list gets the same result.
#[derive(Clone, Default)]
pub(crate) struct ProposalSpends(Arc<Mutex<VecDeque<Entry>>>);

impl ProposalSpends {
    /// Remembers the spent outputs of a proposal that passed the spend check.
    pub(crate) fn remember(
        &self,
        proposal: &SemanticallyVerifiedBlock,
        spent_utxos: &HashMap<transparent::OutPoint, transparent::OrderedUtxo>,
    ) {
        let entry = Entry {
            parent_hash: proposal.block.header.previous_block_hash,
            transaction_hashes: proposal.transaction_hashes.clone(),
            spent_utxos: spent_utxos.clone(),
        };
        let mut entries = self.lock();
        entries.retain(|old| !old.matches(proposal));
        if entries.len() == MAX_ENTRIES {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    /// Removes and returns the spent outputs of a proposal with `block`'s parent and
    /// transactions, if one was validated.
    pub(crate) fn take(
        &self,
        block: &SemanticallyVerifiedBlock,
    ) -> Option<HashMap<transparent::OutPoint, transparent::OrderedUtxo>> {
        let mut entries = self.lock();
        let index = entries.iter().position(|entry| entry.matches(block))?;
        entries.remove(index).map(|entry| entry.spent_utxos)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<Entry>> {
        // The lock is only held for simple list operations that cannot panic midway.
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Entry {
    fn matches(&self, block: &SemanticallyVerifiedBlock) -> bool {
        self.parent_hash == block.block.header.previous_block_hash
            && self.transaction_hashes == block.transaction_hashes
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use zakura_chain::{
        amount::NonNegative, block::Block, parameters::Network, value_balance::ValueBalance,
    };

    use crate::{
        arbitrary::Prepare,
        service::{finalized_state::FinalizedState, non_finalized_state::NonFinalizedState},
        tests::FakeChainHelper,
        Config,
    };

    use super::{ProposalSpends, MAX_ENTRIES};

    fn blocks() -> (Arc<Block>, Arc<Block>, Arc<Block>) {
        // Pre-Heartwood blocks, so an empty finalized state needs no history tree.
        let parent: Arc<Block> = Arc::new(
            Network::Mainnet
                .test_block(653599, 583999)
                .expect("the test vector exists"),
        );
        let child = parent.make_fake_child().set_work(10);
        let other_child = parent.make_fake_child().set_work(1).make_fake_child();
        (parent, child, other_child)
    }

    #[test]
    fn a_result_is_reused_only_for_the_same_parent_and_transactions() {
        let _init_guard = zakura_test::init();
        let (parent, child, other_child) = blocks();
        let spends = ProposalSpends::default();
        let empty = Default::default();

        spends.remember(&child.clone().prepare(), &empty);

        assert!(spends.take(&other_child.prepare()).is_none());
        let mut more_transactions = Block::clone(&child);
        more_transactions
            .transactions
            .push(more_transactions.transactions[0].clone());
        assert!(
            spends
                .take(&Arc::new(more_transactions).prepare())
                .is_none(),
            "the same parent with different transactions does not match"
        );
        assert!(spends.take(&parent.prepare()).is_none());
        // A solved block differs from its proposal only in header fields the key leaves out.
        let solved = child.set_work(3);
        assert!(spends.take(&solved.clone().prepare()).is_some());
        assert!(
            spends.take(&solved.prepare()).is_none(),
            "a result is used once"
        );
    }

    #[test]
    fn only_the_latest_results_are_kept() {
        let _init_guard = zakura_test::init();
        let (parent, ..) = blocks();
        let spends = ProposalSpends::default();
        let empty = Default::default();

        let mut block = parent;
        let mut proposals = Vec::new();
        for _ in 0..=MAX_ENTRIES {
            block = block.make_fake_child();
            spends.remember(&block.clone().prepare(), &empty);
            proposals.push(block.clone().prepare());
        }

        assert!(
            spends.take(&proposals[0]).is_none(),
            "the oldest is evicted"
        );
        for proposal in &proposals[1..] {
            assert!(spends.take(proposal).is_some());
        }
    }

    #[test]
    fn a_proposal_clone_records_spends_that_the_commit_then_uses() {
        let _init_guard = zakura_test::init();
        let network = Network::Mainnet;
        let (parent, child, _) = blocks();
        let finalized_state = FinalizedState::new(&Config::ephemeral(), &network)
            .expect("opening an ephemeral database succeeds");
        finalized_state
            .set_finalized_value_pool(ValueBalance::<NonNegative>::fake_populated_pool());

        let mut state = NonFinalizedState::new(&network);
        state
            .commit_new_chain(parent.prepare(), &finalized_state)
            .expect("the parent commits");

        let mut proposal_state = state.clone();
        proposal_state.disable_metrics();
        proposal_state.record_proposal_spends();
        proposal_state
            .commit_block(child.clone().prepare(), &finalized_state)
            .expect("the proposal is valid");

        let recorded = state
            .proposal_spends
            .take(&child.clone().prepare())
            .expect("the proposal clone shares its results with the original state");
        state
            .proposal_spends
            .remember(&child.clone().prepare(), &recorded);

        state
            .commit_block(child.clone().prepare(), &finalized_state)
            .expect("the solved block commits");
        assert!(
            state.proposal_spends.take(&child.prepare()).is_none(),
            "the commit used the proposal's result"
        );
    }
}
