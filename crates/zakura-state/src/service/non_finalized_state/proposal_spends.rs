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
    use std::{collections::HashMap, sync::Arc};

    use zakura_chain::{
        amount::{Amount, NonNegative},
        block::{Block, CommitmentError, Height},
        parameters::{Network, NetworkUpgrade},
        serialization::ZcashDeserializeInto,
        transaction::{LockTime, Transaction},
        transparent,
        value_balance::ValueBalance,
    };

    use crate::{
        arbitrary::Prepare,
        service::{
            check,
            finalized_state::FinalizedState,
            non_finalized_state::{ContextualMetrics, NonFinalizedState},
        },
        tests::FakeChainHelper,
        Config, ValidateContextError,
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

    fn spend_transaction(version: u32, outpoint: transparent::OutPoint) -> Transaction {
        let inputs = vec![transparent::Input::PrevOut {
            outpoint,
            unlock_script: transparent::Script::new(&[0x51]),
            sequence: u32::MAX,
        }];
        match version {
            4 => Transaction::V4 {
                inputs,
                outputs: vec![],
                lock_time: LockTime::unlocked(),
                expiry_height: Height(0),
                joinsplit_data: None,
                sapling_shielded_data: None,
            },
            5 => Transaction::V5 {
                network_upgrade: NetworkUpgrade::Nu5,
                inputs,
                outputs: vec![],
                lock_time: LockTime::unlocked(),
                expiry_height: Height(0),
                sapling_shielded_data: None,
                orchard_shielded_data: None,
            },
            6 => Transaction::V6 {
                network_upgrade: NetworkUpgrade::Nu7,
                inputs,
                outputs: vec![],
                lock_time: LockTime::unlocked(),
                expiry_height: Height(0),
                sapling_shielded_data: None,
                orchard_shielded_data: None,
                ironwood_shielded_data: None,
            },
            _ => unreachable!("the tests cover transaction versions 4, 5, and 6"),
        }
    }

    /// Builds contextual-validation inputs; signatures and the transaction's shielded
    /// destination are outside this state-layer test, as they are checked by consensus.
    fn state_with_spend() -> (
        FinalizedState,
        NonFinalizedState,
        Arc<Block>,
        transparent::OutPoint,
        transparent::OrderedUtxo,
    ) {
        let (parent, ..) = blocks();
        let outpoint = transparent::OutPoint {
            hash: parent.transactions[0].hash(),
            index: 0,
        };
        let expected = parent.clone().prepare().new_outputs[&outpoint].clone();
        assert!(expected.utxo.output.value > Amount::<NonNegative>::zero());
        assert!(!expected.utxo.output.lock_script.as_raw_bytes().is_empty());

        let finalized = FinalizedState::new(&Config::ephemeral(), &Network::Mainnet)
            .expect("opening an ephemeral database succeeds");
        finalized.set_finalized_value_pool(ValueBalance::<NonNegative>::fake_populated_pool());
        let mut state = NonFinalizedState::new(&Network::Mainnet);
        state
            .commit_new_chain(parent.clone().prepare(), &finalized)
            .expect("the parent commits");
        let mut tip = parent;
        for _ in 0..crate::constants::MIN_TRANSPARENT_COINBASE_MATURITY {
            tip = tip.make_fake_child();
            state
                .commit_block(tip.clone().prepare(), &finalized)
                .expect("the coinbase matures on the parent chain");
        }
        let mut child = tip.make_fake_child();
        Arc::make_mut(&mut child)
            .transactions
            .push(Arc::new(spend_transaction(4, outpoint)));
        (finalized, state, child, outpoint, expected)
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
    fn transaction_order_and_parent_are_each_part_of_the_cache_key() {
        let _init_guard = zakura_test::init();
        let (parent, mut child, _) = blocks();
        Arc::make_mut(&mut child)
            .transactions
            .push(parent.transactions[0].clone());
        let spends = ProposalSpends::default();
        spends.remember(&child.clone().prepare(), &HashMap::new());
        let mut reordered = child.clone();
        Arc::make_mut(&mut reordered).transactions.swap(0, 1);
        assert!(spends.take(&reordered.prepare()).is_none());
        let mut different_parent = child.clone();
        Arc::make_mut(&mut Arc::make_mut(&mut different_parent).header).previous_block_hash =
            parent.header.previous_block_hash;
        assert!(spends.take(&different_parent.prepare()).is_none());
        assert!(spends.take(&child.prepare()).is_some());
    }

    #[test]
    fn a_proposal_clone_records_spends_that_the_commit_then_uses() {
        let _init_guard = zakura_test::init();
        let (finalized_state, mut state, child, outpoint, expected) = state_with_spend();
        // Independently constructed state has no shared cache with the proposal.
        let (_, mut uncached_state, _, _, _) = state_with_spend();
        assert!(uncached_state
            .proposal_spends
            .take(&child.clone().prepare())
            .is_none());
        uncached_state
            .commit_block(child.clone().prepare(), &finalized_state)
            .expect("the uncached spend is valid");

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
        assert_eq!(recorded, HashMap::from([(outpoint, expected)]));
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
        let cached_chain = state.best_chain().expect("the solved block committed");
        let uncached_chain = uncached_state
            .best_chain()
            .expect("the uncached block committed");
        assert!(cached_chain.eq_internal_state(uncached_chain));
        assert!(!cached_chain.unspent_utxos().contains_key(&outpoint));
    }

    #[test]
    fn a_repeated_proposal_preserves_the_cached_result_when_its_spend_check_fails() {
        let _init_guard = zakura_test::init();
        let (finalized, mut state, child, outpoint, expected) = state_with_spend();
        let mut proposal_state = state.clone();
        proposal_state.record_proposal_spends();
        proposal_state
            .commit_block(child.clone().prepare(), &finalized)
            .expect("the first proposal is valid and records its spent output");

        // Hide the output only from the repeated proposal's parent snapshot to force
        // a lookup failure. The writer retains the valid parent and cached result.
        // This makes cache preservation observable without relying on thread timing.
        let mut proposal_chain = state
            .best_chain()
            .expect("the parent is present")
            .as_ref()
            .clone();
        proposal_chain.created_utxos.remove(&outpoint);
        let result = proposal_state.validate_and_commit(
            Arc::new(proposal_chain),
            child.clone().prepare(),
            &finalized,
            ContextualMetrics::Disabled,
        );
        assert!(matches!(
            result,
            Err(ValidateContextError::MissingTransparentOutput { outpoint: missing, .. }) if missing == outpoint
        ));
        let recorded = state
            .proposal_spends
            .take(&child.clone().prepare())
            .expect("a repeated proposal must leave the writer's cached result available");
        assert_eq!(recorded, HashMap::from([(outpoint, expected)]));
        state
            .proposal_spends
            .remember(&child.clone().prepare(), &recorded);
        state
            .commit_block(child.clone().prepare(), &finalized)
            .expect("the writer can still commit using the cached spend result");
        assert!(state.proposal_spends.take(&child.prepare()).is_none());
    }

    #[test]
    fn an_invalid_spend_on_a_cache_miss_is_still_rejected() {
        let _init_guard = zakura_test::init();
        let (finalized, mut state, child, outpoint, _) = state_with_spend();
        let mut proposal_state = state.clone();
        proposal_state.record_proposal_spends();
        proposal_state
            .commit_block(child.clone().prepare(), &finalized)
            .unwrap();
        let before = state.clone();
        let missing = transparent::OutPoint {
            index: u32::MAX,
            ..outpoint
        };
        let mut invalid = child.clone();
        Arc::make_mut(&mut invalid).transactions[1] = Arc::new(spend_transaction(4, missing));
        assert!(state
            .proposal_spends
            .take(&invalid.clone().prepare())
            .is_none());
        assert!(matches!(
            state.commit_block(invalid.prepare(), &finalized),
            Err(ValidateContextError::MissingTransparentOutput { outpoint, .. }) if outpoint == missing
        ));
        assert!(state.eq_internal_state(&before));
        assert!(state.proposal_spends.take(&child.prepare()).is_some());
    }

    #[test]
    fn a_failed_proposal_does_not_bypass_later_checks_on_a_cache_hit() {
        let _init_guard = zakura_test::init();
        let network = Network::Mainnet;
        let height = NetworkUpgrade::Heartwood
            .activation_height(&network)
            .unwrap();
        let parent: Arc<Block> = network.block_map()[&(height.0 - 1)]
            .zcash_deserialize_into()
            .unwrap();
        let finalized = FinalizedState::new(&Config::ephemeral(), &network).unwrap();
        let mut state = NonFinalizedState::new(&network);
        state
            .commit_new_chain(parent.clone().prepare(), &finalized)
            .unwrap();
        let before = state.clone();
        // Heartwood's activation block requires an all-zero reserved commitment.
        let invalid = parent.make_fake_child().set_block_commitment([1; 32]);
        let mut proposal_state = state.clone();
        proposal_state.record_proposal_spends();
        let proposal_error = proposal_state.commit_block(invalid.clone().prepare(), &finalized);
        assert!(matches!(
            proposal_error,
            Err(ValidateContextError::InvalidBlockCommitment(
                CommitmentError::InvalidChainHistoryActivationReserved { .. }
            ))
        ));
        let recorded = state
            .proposal_spends
            .take(&invalid.clone().prepare())
            .expect("the spend check succeeded before the proposal's commitment failed");
        state
            .proposal_spends
            .remember(&invalid.clone().prepare(), &recorded);
        assert_eq!(
            state.commit_block(invalid.clone().prepare(), &finalized),
            proposal_error
        );
        assert!(state.proposal_spends.take(&invalid.prepare()).is_none());
        assert!(state.eq_internal_state(&before));
        state
            .commit_block(
                parent
                    .make_fake_child()
                    .set_block_commitment([0; 32])
                    .prepare(),
                &finalized,
            )
            .expect("the corrected commitment is accepted");
    }

    #[test]
    fn modern_authorization_changes_match_but_spend_effect_changes_do_not() {
        let _init_guard = zakura_test::init();
        let (finalized, state, child, outpoint, expected) = state_with_spend();
        let chain = state.best_chain().expect("the parent chain is present");
        for version in [5, 6] {
            let mut proposal = child.clone();
            Arc::make_mut(&mut proposal).transactions[1] =
                Arc::new(spend_transaction(version, outpoint));
            let spends = ProposalSpends::default();
            let recorded = HashMap::from([(outpoint, expected.clone())]);
            assert_eq!(
                check::utxo::transparent_spend(
                    &proposal.clone().prepare(),
                    &chain.created_utxos,
                    &chain.spent_utxos,
                    &finalized,
                ),
                Ok(recorded.clone())
            );
            spends.remember(&proposal.clone().prepare(), &recorded);

            let mut authorized = proposal.clone();
            let tx = Arc::make_mut(&mut Arc::make_mut(&mut authorized).transactions[1]);
            let transparent::Input::PrevOut { unlock_script, .. } = &mut tx.inputs_mut()[0] else {
                unreachable!("the fixture spends a prior output")
            };
            *unlock_script = transparent::Script::new(&[0x52]);
            assert_eq!(
                proposal.transactions[1].hash(),
                authorized.transactions[1].hash()
            );
            assert_ne!(
                proposal.transactions[1].auth_digest(),
                authorized.transactions[1].auth_digest()
            );
            assert_eq!(
                check::utxo::transparent_spend(
                    &authorized.clone().prepare(),
                    &chain.created_utxos,
                    &chain.spent_utxos,
                    &finalized,
                ),
                Ok(recorded.clone())
            );
            assert_eq!(spends.take(&authorized.prepare()), Some(recorded.clone()));

            for change_input in [true, false] {
                spends.remember(&proposal.clone().prepare(), &recorded);
                let mut changed = proposal.clone();
                let tx = Arc::make_mut(&mut Arc::make_mut(&mut changed).transactions[1]);
                if change_input {
                    tx.inputs_mut()[0].set_outpoint(transparent::OutPoint {
                        index: u32::MAX,
                        ..outpoint
                    });
                } else {
                    tx.outputs_mut().push(expected.utxo.output.clone());
                }
                assert_ne!(
                    proposal.transactions[1].hash(),
                    changed.transactions[1].hash()
                );
                assert!(spends.take(&changed.prepare()).is_none());
                assert_eq!(
                    spends.take(&proposal.clone().prepare()),
                    Some(recorded.clone())
                );
            }
        }
    }
}
