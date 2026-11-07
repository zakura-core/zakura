//! Tests for reusing a validated proposal's results when its solved block commits.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{Block, CommitmentError, Hash, Height},
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
        non_finalized_state::{Chain, ContextualMetrics, NonFinalizedState},
    },
    tests::FakeChainHelper,
    Config, ContextuallyVerifiedBlock, SemanticallyVerifiedBlock, ValidateContextError,
};

use super::{
    bound_prepared_chains, Entry, NotPrepared, PreparedProposal, ProposalResults, MAX_ENTRIES,
    MAX_PREPARED_CHAINS,
};

/// Publishes through the writer boundary without keeping an extra snapshot alive in the fixture.
fn publish(state: &NonFinalizedState) {
    let (sender, _receiver) = tokio::sync::watch::channel(state.clone());
    state.publish(&sender);
}

/// The outputs a proposal left for `block`'s spend check, if any.
fn take_spends(
    results: &ProposalResults,
    block: &SemanticallyVerifiedBlock,
) -> Option<Arc<HashMap<transparent::OutPoint, transparent::OrderedUtxo>>> {
    results.take(block).map(|entry| entry.spent_utxos)
}

fn contextual(
    proposal: SemanticallyVerifiedBlock,
    spent_outputs: &Arc<HashMap<transparent::OutPoint, transparent::OrderedUtxo>>,
) -> ContextuallyVerifiedBlock {
    ContextuallyVerifiedBlock::with_block_and_shared_spent_utxos(
        &Network::Mainnet,
        proposal,
        Arc::clone(spent_outputs),
    )
    .expect("the cache fixture has consistent spent outputs and value balances")
}

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
    publish(&state);
    (finalized, state, child, outpoint, expected)
}

/// Solves `proposal`: mining sets the nonce and the solution, and may move the time on, each of
/// which changes the block hash and the history tree leaf.
fn solve(proposal: &Arc<Block>) -> Arc<Block> {
    let mut solved = Block::clone(proposal);
    let header = Arc::make_mut(&mut solved.header);
    header.nonce = [0x5a; 32].into();
    header.time += chrono::Duration::seconds(1);
    Arc::new(solved)
}

/// A Heartwood-activation fixture: a one-block chain whose next block's header commits to the
/// chain history, so the block commitment check does real work.
fn heartwood_state() -> (FinalizedState, NonFinalizedState, Arc<Block>) {
    let network = Network::Mainnet;
    let height = NetworkUpgrade::Heartwood
        .activation_height(&network)
        .expect("Heartwood activates on Mainnet");
    let parent: Arc<Block> = network.block_map()[&(height.0 - 1)]
        .zcash_deserialize_into()
        .expect("the test vector is a valid block");
    let finalized = FinalizedState::new(&Config::ephemeral(), &network)
        .expect("opening an ephemeral database succeeds");
    let mut state = NonFinalizedState::new(&network);
    state
        .commit_new_chain(parent.clone().prepare(), &finalized)
        .expect("the parent commits");
    // Heartwood's activation block requires an all-zero reserved commitment.
    let child = parent.make_fake_child().set_block_commitment([0; 32]);
    publish(&state);
    (finalized, state, child)
}

/// Replaces the fixture's spend with one of `version`, so that changing its unlock script
/// changes the authorizing data without changing the transaction ID.
fn with_spend_version(
    child: &Arc<Block>,
    version: u32,
    outpoint: transparent::OutPoint,
) -> Arc<Block> {
    let mut block = Block::clone(child);
    block.transactions[1] = Arc::new(spend_transaction(version, outpoint));
    Arc::new(block)
}

/// Validates `proposal` on a clone of `state`, as the read service does, and returns the parent
/// chain with the chain the proposal's update produced.
fn prepare(
    state: &NonFinalizedState,
    finalized: &FinalizedState,
    proposal: SemanticallyVerifiedBlock,
) -> (Arc<Chain>, Arc<Chain>) {
    let parent_chain = state
        .best_chain()
        .expect("the parent chain is present")
        .clone();
    let mut proposal_state = state.clone();
    proposal_state.disable_metrics();
    proposal_state
        .commit_block(proposal, finalized)
        .expect("the proposal is valid");
    let child_chain = proposal_state
        .best_chain()
        .expect("the proposal committed")
        .clone();
    (parent_chain, child_chain)
}

#[test]
fn a_result_is_reused_only_for_the_same_parent_and_transactions() {
    let _init_guard = zakura_test::init();
    let (parent, child, other_child) = blocks();
    let results = ProposalResults::default();
    let empty = Default::default();

    results.remember(&contextual(child.clone().prepare(), &empty), None);

    assert!(take_spends(&results, &other_child.prepare()).is_none());
    let mut more_transactions = Block::clone(&child);
    more_transactions
        .transactions
        .push(more_transactions.transactions[0].clone());
    assert!(
        take_spends(&results, &Arc::new(more_transactions).prepare()).is_none(),
        "the same parent with different transactions does not match"
    );
    assert!(take_spends(&results, &parent.prepare()).is_none());
    // A solved block differs from its proposal only in header fields the key leaves out.
    let solved = child.set_work(3);
    assert!(take_spends(&results, &solved.clone().prepare()).is_some());
    assert!(
        take_spends(&results, &solved.prepare()).is_none(),
        "a result is used once"
    );
}

#[test]
fn only_the_latest_results_are_kept() {
    let _init_guard = zakura_test::init();
    let (parent, ..) = blocks();
    let results = ProposalResults::default();
    let empty = Default::default();

    let mut block = parent;
    let mut proposals = Vec::new();
    for _ in 0..=MAX_ENTRIES {
        block = block.make_fake_child();
        results.remember(&contextual(block.clone().prepare(), &empty), None);
        proposals.push(block.clone().prepare());
    }

    assert!(
        take_spends(&results, &proposals[0]).is_none(),
        "the oldest is evicted"
    );
    for proposal in &proposals[1..] {
        assert!(take_spends(&results, proposal).is_some());
    }
}

#[test]
fn eviction_and_cache_drop_preserve_spends_retained_by_validation() {
    let _init_guard = zakura_test::init();
    let (_, _, mut child, outpoint, expected) = state_with_spend();
    let results = ProposalResults::default();
    let recorded = Arc::new(HashMap::from([(outpoint, expected.clone())]));
    let original = child.clone().prepare();
    results.remember(&contextual(original.clone(), &recorded), None);
    assert_eq!(Arc::strong_count(&recorded), 2);

    for _ in 0..MAX_ENTRIES {
        child = child.make_fake_child();
        results.remember(&contextual(child.clone().prepare(), &Arc::default()), None);
    }
    assert!(
        take_spends(&results, &original).is_none(),
        "the oldest result was evicted"
    );
    assert_eq!(Arc::strong_count(&recorded), 1);
    drop(results);
    assert_eq!(recorded.as_ref(), &HashMap::from([(outpoint, expected)]));
}

#[test]
fn transaction_order_and_parent_are_each_part_of_the_cache_key() {
    let _init_guard = zakura_test::init();
    let (parent, mut child, _) = blocks();
    Arc::make_mut(&mut child)
        .transactions
        .push(parent.transactions[0].clone());
    let results = ProposalResults::default();
    results.remember(
        &contextual(child.clone().prepare(), &Arc::new(HashMap::new())),
        None,
    );
    let mut reordered = child.clone();
    Arc::make_mut(&mut reordered).transactions.swap(0, 1);
    assert!(take_spends(&results, &reordered.prepare()).is_none());
    let mut different_parent = child.clone();
    Arc::make_mut(&mut Arc::make_mut(&mut different_parent).header).previous_block_hash =
        parent.header.previous_block_hash;
    assert!(take_spends(&results, &different_parent.prepare()).is_none());
    assert!(take_spends(&results, &child.prepare()).is_some());
}

#[test]
fn a_proposal_clone_records_spends_that_the_commit_then_uses() {
    let _init_guard = zakura_test::init();
    let (finalized_state, mut state, child, outpoint, expected) = state_with_spend();
    // Independently constructed state has no shared cache with the proposal.
    let (_, mut uncached_state, _, _, _) = state_with_spend();
    assert!(take_spends(&uncached_state.proposal_results, &child.clone().prepare()).is_none());
    uncached_state
        .commit_block(child.clone().prepare(), &finalized_state)
        .expect("the uncached spend is valid");

    let mut proposal_state = state.clone();
    proposal_state.disable_metrics();
    proposal_state.record_proposal_results();
    proposal_state
        .commit_block(child.clone().prepare(), &finalized_state)
        .expect("the proposal is valid");

    let recorded = take_spends(&state.proposal_results, &child.clone().prepare())
        .expect("the proposal clone shares its results with the original state");
    assert_eq!(recorded.as_ref(), &HashMap::from([(outpoint, expected)]));
    assert!(Arc::ptr_eq(
        &recorded,
        &proposal_state
            .best_tip_block()
            .expect("the proposal committed")
            .spent_outputs,
    ));
    state.proposal_results.remember(
        proposal_state
            .best_tip_block()
            .expect("the valid proposal committed"),
        None,
    );

    state
        .commit_block(child.clone().prepare(), &finalized_state)
        .expect("the solved block commits");
    assert!(
        take_spends(&state.proposal_results, &child.prepare()).is_none(),
        "the commit used the proposal's result"
    );
    let cached_chain = state.best_chain().expect("the solved block committed");
    assert!(Arc::ptr_eq(
        &recorded,
        &cached_chain
            .tip_block()
            .expect("the solved block committed")
            .spent_outputs,
    ));
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
    proposal_state.record_proposal_results();
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
        None,
    );
    assert!(matches!(
        result,
        Err(ValidateContextError::MissingTransparentOutput { outpoint: missing, .. }) if missing == outpoint
    ));
    let recorded = take_spends(&state.proposal_results, &child.clone().prepare())
        .expect("a repeated proposal must leave the writer's cached result available");
    assert_eq!(recorded.as_ref(), &HashMap::from([(outpoint, expected)]));
    state.proposal_results.remember(
        proposal_state
            .best_tip_block()
            .expect("the first valid proposal committed"),
        None,
    );
    state
        .commit_block(child.clone().prepare(), &finalized)
        .expect("the writer can still commit using the cached spend result");
    assert!(take_spends(&state.proposal_results, &child.prepare()).is_none());
}

#[test]
fn an_invalid_spend_on_a_cache_miss_is_still_rejected() {
    let _init_guard = zakura_test::init();
    let (finalized, mut state, child, outpoint, _) = state_with_spend();
    let mut proposal_state = state.clone();
    proposal_state.record_proposal_results();
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
    assert!(take_spends(&state.proposal_results, &invalid.clone().prepare()).is_none());
    assert!(matches!(
        state.commit_block(invalid.prepare(), &finalized),
        Err(ValidateContextError::MissingTransparentOutput { outpoint, .. }) if outpoint == missing
    ));
    assert!(state.eq_internal_state(&before));
    assert!(take_spends(&state.proposal_results, &child.prepare()).is_some());
}

#[test]
fn failed_proposals_leave_the_cache_empty_or_preserve_a_matching_valid_result() {
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
    proposal_state.record_proposal_results();
    let proposal_error = proposal_state.commit_block(invalid.clone().prepare(), &finalized);
    assert!(matches!(
        proposal_error,
        Err(ValidateContextError::InvalidBlockCommitment(
            CommitmentError::InvalidChainHistoryActivationReserved { .. }
        ))
    ));
    assert!(
        take_spends(&state.proposal_results, &invalid.clone().prepare()).is_none(),
        "a rejected proposal must not populate the cache"
    );

    let valid = parent.make_fake_child().set_block_commitment([0; 32]);
    proposal_state
        .commit_block(valid.clone().prepare(), &finalized)
        .expect("the corrected proposal passes every contextual check");
    let recorded = take_spends(&state.proposal_results, &valid.clone().prepare())
        .expect("a fully validated proposal records its spent outputs");
    state.proposal_results.remember(
        proposal_state
            .best_tip_block()
            .expect("the valid proposal committed"),
        None,
    );

    // Only the header commitment differs, so the invalid proposal has the same
    // spend-cache key. Its later failure must not replace the successful result.
    assert_eq!(
        proposal_state.commit_block(invalid.clone().prepare(), &finalized),
        proposal_error
    );
    let retained = take_spends(&state.proposal_results, &invalid.clone().prepare())
        .expect("the rejected proposal leaves the earlier successful result available");
    assert!(Arc::ptr_eq(&recorded, &retained));
    state.proposal_results.remember(
        proposal_state
            .best_tip_block()
            .expect("the failed proposal leaves the valid tip intact"),
        None,
    );

    // A solved block still runs the later check even when it consumes a valid
    // proposal's cached spend result.
    assert_eq!(
        state.commit_block(invalid.clone().prepare(), &finalized),
        proposal_error
    );
    assert!(take_spends(&state.proposal_results, &invalid.prepare()).is_none());
    assert!(state.eq_internal_state(&before));
    state
        .commit_block(valid.prepare(), &finalized)
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
        let results = ProposalResults::default();
        let recorded = Arc::new(HashMap::from([(outpoint, expected.clone())]));
        assert_eq!(
            check::utxo::transparent_spend(
                &proposal.clone().prepare(),
                &chain.created_utxos,
                &chain.spent_utxos,
                &finalized,
            )
            .map(Arc::new),
            Ok(recorded.clone())
        );
        results.remember(&contextual(proposal.clone().prepare(), &recorded), None);

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
            )
            .map(Arc::new),
            Ok(recorded.clone())
        );
        assert_eq!(
            take_spends(&results, &authorized.prepare()),
            Some(recorded.clone())
        );

        for change_input in [true, false] {
            results.remember(&contextual(proposal.clone().prepare(), &recorded), None);
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
            assert!(take_spends(&results, &changed.prepare()).is_none());
            assert_eq!(
                take_spends(&results, &proposal.clone().prepare()),
                Some(recorded.clone())
            );
        }
    }
}

// Reusing the chain a proposal built.

/// The writer's commit of a solved block must finish the chain its proposal built, and reach the
/// same state as a commit that had no proposal to reuse.
#[test]
fn a_solved_block_commits_from_the_chain_its_proposal_built() {
    let _init_guard = zakura_test::init();
    let (finalized, mut state, child, _, _) = state_with_spend();
    // An independently built state shares no cache with the proposal.
    let (uncached_finalized, mut uncached_state, _, _, _) = state_with_spend();

    let mut proposal_state = state.clone();
    proposal_state.disable_metrics();
    proposal_state.record_proposal_results();
    proposal_state
        .commit_block(child.clone().prepare(), &finalized)
        .expect("the proposal is valid");
    let proposal_outputs = Arc::clone(
        &proposal_state
            .best_tip_block()
            .expect("the proposal committed")
            .new_outputs,
    );

    let solved = solve(&child);
    assert_ne!(solved.hash(), child.hash());

    state
        .commit_block(solved.clone().prepare(), &finalized)
        .expect("the solved block commits");
    uncached_state
        .commit_block(solved.clone().prepare(), &uncached_finalized)
        .expect("the same block commits without a proposal to reuse");

    let committed = state.best_chain().expect("the solved block committed");
    assert_eq!(committed.non_finalized_tip_hash(), solved.hash());
    assert!(
        Arc::ptr_eq(
            &committed
                .tip_block()
                .expect("the solved block committed")
                .new_outputs,
            &proposal_outputs,
        ),
        "the commit must reuse the outputs the proposal already built, not build them again",
    );
    assert!(
        committed.eq_internal_state(
            uncached_state
                .best_chain()
                .expect("the uncached block committed")
        ),
        "finishing the proposal's chain must reach the same state as an ordinary commit",
    );
}

/// A parent chain with the same contents but a different allocation is a different chain: the
/// writer may have replaced it by growing, finalizing, reorganizing, or invalidating.
#[test]
fn a_changed_parent_chain_is_not_reused() {
    let _init_guard = zakura_test::init();
    let (finalized, state, child, _, _) = state_with_spend();
    let (parent_chain, prepared) = prepare(&state, &finalized, child.clone().prepare());
    let solved = solve(&child).prepare();

    let copied_parent = Arc::new(parent_chain.as_ref().clone());
    assert!(
        copied_parent.eq_internal_state(&parent_chain),
        "the copy holds the same chain state",
    );
    assert_eq!(
        PreparedProposal::new(&parent_chain, &prepared)
            .for_solved_block(&solved, &copied_parent)
            .err(),
        Some(NotPrepared::ParentChanged),
    );

    assert!(
        PreparedProposal::new(&parent_chain, &prepared)
            .for_solved_block(&solved, &parent_chain)
            .is_ok(),
        "the chain the proposal extended is reused",
    );
}

/// Holding the parent chain is what makes pointer equality mean "unchanged": a non-finalized
/// state keeps its chains behind `Arc`s in a set, so a held chain cannot be changed in place and
/// every change replaces it.
#[test]
fn a_held_parent_chain_is_replaced_rather_than_changed() {
    let _init_guard = zakura_test::init();
    let (finalized, mut state, child, _, _) = state_with_spend();

    let held = state
        .best_chain()
        .expect("the parent chain is present")
        .clone();
    state
        .commit_block(child.prepare(), &finalized)
        .expect("the block commits");
    let grown = state.best_chain().expect("the block committed");
    assert!(
        !Arc::ptr_eq(&held, grown),
        "growing the chain replaced the held chain rather than changing it",
    );

    let held = grown.clone();
    state.finalize();
    let finalized_once = state.best_chain().expect("the chain is not empty");
    assert!(
        !Arc::ptr_eq(&held, finalized_once),
        "finalizing the root replaced the held chain rather than changing it",
    );
}

/// A block that reaches the parent chain between the proposal and the solved block replaces the
/// chain the proposal extended, so the solved block commits the ordinary way, as a fork of the
/// grown chain, and still reaches the state a commit with no proposal to reuse reaches.
#[test]
fn a_solved_block_whose_parent_chain_grew_commits_the_ordinary_way() {
    let _init_guard = zakura_test::init();
    let (finalized, mut state, child, _, _) = state_with_spend();
    // An independently built state shares no cache with the proposal.
    let (uncached_finalized, mut uncached_state, _, _, _) = state_with_spend();

    let mut proposal_state = state.clone();
    proposal_state.disable_metrics();
    proposal_state.record_proposal_results();
    proposal_state
        .commit_block(child.clone().prepare(), &finalized)
        .expect("the proposal is valid");
    let proposal_outputs = Arc::clone(
        &proposal_state
            .best_tip_block()
            .expect("the proposal committed")
            .new_outputs,
    );

    // A synced sibling of the proposal reaches the parent chain first. A fake child's hash comes
    // from its header alone, so the sibling needs its own header to differ from the proposal.
    let sibling = state
        .best_tip_block()
        .expect("the parent chain is present")
        .block
        .make_fake_child()
        .set_work(2);
    assert_ne!(sibling.hash(), child.hash());
    state
        .commit_block(sibling.clone().prepare(), &finalized)
        .expect("the sibling commits");
    uncached_state
        .commit_block(sibling.prepare(), &uncached_finalized)
        .expect("the sibling commits without a proposal");

    let solved = solve(&child);
    state
        .commit_block(solved.clone().prepare(), &finalized)
        .expect("the solved block commits as a fork of the grown chain");
    uncached_state
        .commit_block(solved.clone().prepare(), &uncached_finalized)
        .expect("the same block commits without a proposal to reuse");

    let solved_chain = |state: &NonFinalizedState| {
        state
            .find_chain(|chain| chain.non_finalized_tip_hash() == solved.hash())
            .expect("the solved block committed")
    };
    let committed = solved_chain(&state);
    assert!(
        !Arc::ptr_eq(
            &committed
                .tip_block()
                .expect("the solved block committed")
                .new_outputs,
            &proposal_outputs,
        ),
        "the parent chain changed, so the commit must not finish the proposal's chain",
    );
    assert!(
        committed.eq_internal_state(&solved_chain(&uncached_state)),
        "the ordinary commit of a solved block after its parent chain grew must reach the same \
         state as a commit with no proposal to reuse",
    );
}

/// Removing a finalized root must prevent reuse even when the parent tip is unchanged.
#[test]
fn a_solved_block_after_root_finalization_uses_ordinary_validation() {
    let _init_guard = zakura_test::init();
    let (finalized, mut state, mut child, _, _) = state_with_spend();
    let (uncached_finalized, mut uncached_state, _, _, _) = state_with_spend();
    // Exercise the non-finalized transition without writing the synthetic root to disk.
    // The proposal must not spend that root's output after it leaves memory.
    Arc::make_mut(&mut child).transactions.truncate(1);

    let mut proposal_state = state.clone();
    proposal_state.disable_metrics();
    proposal_state.record_proposal_results();
    proposal_state
        .commit_block(child.clone().prepare(), &finalized)
        .expect("the proposal is valid");
    let proposal_outputs = Arc::clone(
        &proposal_state
            .best_tip_block()
            .expect("the proposal committed")
            .new_outputs,
    );
    let parent_hash = child.header.previous_block_hash;
    state.finalize();
    uncached_state.finalize();
    assert_eq!(
        state.best_chain().unwrap().non_finalized_tip_hash(),
        parent_hash
    );

    let solved = solve(&child);
    state
        .commit_block(solved.clone().prepare(), &finalized)
        .expect("the solved block commits after the root is finalized");
    uncached_state
        .commit_block(solved.prepare(), &uncached_finalized)
        .expect("the same block commits without a cached proposal");
    assert!(
        !Arc::ptr_eq(
            &state.best_tip_block().unwrap().new_outputs,
            &proposal_outputs,
        ),
        "finalizing the root must prevent reuse of the prepared chain",
    );
    assert!(
        state.eq_internal_state(&uncached_state),
        "fallback after finalization must match an uncached commit",
    );
}

/// A retained proposal must not resurrect a parent removed by operator invalidation.
#[test]
fn a_solved_block_with_an_invalidated_parent_is_rejected() {
    let _init_guard = zakura_test::init();
    let (finalized, mut state, child, _, _) = state_with_spend();
    let (uncached_finalized, mut uncached_state, _, _, _) = state_with_spend();
    let mut proposal_state = state.clone();
    proposal_state.disable_metrics();
    proposal_state.record_proposal_results();
    proposal_state
        .commit_block(child.clone().prepare(), &finalized)
        .expect("the proposal is valid");

    let parent_hash = child.header.previous_block_hash;
    state
        .invalidate_block(parent_hash)
        .expect("the parent exists");
    uncached_state
        .invalidate_block(parent_hash)
        .expect("the uncached parent exists");
    let before = state.clone();
    let solved = solve(&child);
    for (candidate, db) in [
        (&mut state, &finalized),
        (&mut uncached_state, &uncached_finalized),
    ] {
        assert!(matches!(
            candidate.commit_block(solved.clone().prepare(), db),
            Err(ValidateContextError::NotReadyToBeCommitted),
        ));
        assert!(!candidate.any_chain_contains(&solved.hash()));
        assert!(candidate.is_invalidated(&parent_hash));
    }
    assert!(
        state.eq_internal_state(&before),
        "rejection must not change the chain"
    );
    assert!(state.eq_internal_state(&uncached_state));
}

/// Transaction IDs leave out authorizing data, so they are not enough on their own: the
/// proposal's verification covers only the body it verified.
#[test]
fn a_body_that_differs_only_in_authorizing_data_is_not_reused() {
    let _init_guard = zakura_test::init();
    let (finalized, state, child, outpoint, _) = state_with_spend();
    // Only from version 5 does a transaction ID leave the unlock script out.
    let child = with_spend_version(&child, 5, outpoint);
    let (parent_chain, prepared) = prepare(&state, &finalized, child.clone().prepare());

    let mut authorized = solve(&child);
    let tx = Arc::make_mut(&mut Arc::make_mut(&mut authorized).transactions[1]);
    let transparent::Input::PrevOut { unlock_script, .. } = &mut tx.inputs_mut()[0] else {
        unreachable!("the fixture spends a prior output")
    };
    *unlock_script = transparent::Script::new(&[0x52]);
    let authorized = authorized.prepare();
    assert_eq!(
        authorized.transaction_hashes.as_ref(),
        child.clone().prepare().transaction_hashes.as_ref(),
        "the two bodies have the same transaction IDs, so they share a cache key",
    );

    assert_eq!(
        PreparedProposal::new(&parent_chain, &prepared)
            .for_solved_block(&authorized, &parent_chain)
            .err(),
        Some(NotPrepared::TransactionsDiffer),
    );
}

/// The chain's cumulative work comes from the difficulty threshold, which finishing a prepared
/// tip leaves alone, so a solved block that changed it cannot be finished from this chain.
#[test]
fn a_changed_difficulty_threshold_is_not_reused() {
    let _init_guard = zakura_test::init();
    let (finalized, state, child, _, _) = state_with_spend();
    let (parent_chain, prepared) = prepare(&state, &finalized, child.clone().prepare());

    let reworked = solve(&child).set_work(11).prepare();
    assert_ne!(
        reworked.block.header().difficulty_threshold,
        child.header.difficulty_threshold,
    );

    assert_eq!(
        PreparedProposal::new(&parent_chain, &prepared)
            .for_solved_block(&reworked, &parent_chain)
            .err(),
        Some(NotPrepared::DifficultyThresholdDiffers),
    );
}

/// A solved block's own header commitment is checked against its parent's chain history on the
/// prepared path too: the proposal's header committed to a different block.
#[test]
fn the_prepared_path_still_checks_the_solved_block_commitment() {
    let _init_guard = zakura_test::init();
    let (finalized, mut state, child) = heartwood_state();
    let (parent_chain, prepared) = prepare(&state, &finalized, child.clone().prepare());

    let mismatched = solve(&child).set_block_commitment([1; 32]).prepare();
    assert!(matches!(
        state.finish_prepared_chain(
            prepared.clone(),
            mismatched,
            &parent_chain,
            ContextualMetrics::Disabled,
        ),
        Err(ValidateContextError::InvalidBlockCommitment(
            CommitmentError::InvalidChainHistoryActivationReserved { .. }
        )),
    ));

    // The same block with the commitment its header must carry is finished, and reaches the
    // state an ordinary commit of it reaches.
    let solved = solve(&child);
    let finished = state
        .finish_prepared_chain(
            prepared,
            solved.clone().prepare(),
            &parent_chain,
            ContextualMetrics::Disabled,
        )
        .expect("the solved block's header commits to its parent's chain history");
    state
        .commit_block(solved.clone().prepare(), &finalized)
        .expect("the same block commits the ordinary way");
    assert_eq!(finished.non_finalized_tip_hash(), solved.hash());
    assert!(finished.eq_internal_state(
        state
            .best_chain()
            .expect("the ordinary commit added the block")
    ));
}

/// Retained chains cost far more memory than spend results, so only the newest few keep theirs.
/// [`ProposalResults::remember`] applies this bound to every insertion.
#[test]
fn only_the_newest_results_keep_their_chain() {
    let _init_guard = zakura_test::init();
    let (finalized, state, child, _, _) = state_with_spend();
    let (parent_chain, prepared) = prepare(&state, &finalized, child.prepare());

    let mut entries: VecDeque<Entry> = VecDeque::new();
    for index in 0..MAX_ENTRIES {
        entries.push_back(Entry {
            parent_hash: Hash([u8::try_from(index).expect("the bound fits in a byte"); 32]),
            transaction_hashes: Arc::from(Vec::new()),
            spent_utxos: Arc::default(),
            prepared_proposal: Ok(PreparedProposal::new(&parent_chain, &prepared)),
        });
        bound_prepared_chains(&mut entries);
    }

    assert_eq!(entries.len(), MAX_ENTRIES);
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.prepared_proposal.is_ok())
            .count(),
        MAX_PREPARED_CHAINS,
    );
    assert!(
        entries
            .iter()
            .rev()
            .take(MAX_PREPARED_CHAINS)
            .all(|entry| entry.prepared_proposal.is_ok()),
        "the newest entries are the ones that keep their chain",
    );
}

/// The retained chain must be the only remaining reference once the proposal's state is gone, so
/// finishing it takes it rather than copying it. Skipping that copy is most of why the chain is
/// retained at all, so it is checked here rather than assumed.
#[test]
fn a_retained_chain_is_the_only_reference_left_to_it() {
    let _init_guard = zakura_test::init();
    let (finalized, state, child, _, _) = state_with_spend();

    // Validate the proposal exactly as the read service does, on a clone that is then dropped.
    let mut proposal_state = state.clone();
    proposal_state.disable_metrics();
    proposal_state.record_proposal_results();
    proposal_state
        .commit_block(child.clone().prepare(), &finalized)
        .expect("the proposal is valid");
    drop(proposal_state);

    publish(&state);
    let retained = state
        .proposal_results
        .take(&child.prepare())
        .expect("the proposal recorded its results")
        .prepared_proposal
        .expect("the newest proposal keeps its chain");
    assert_eq!(
        Arc::strong_count(&retained.child_chain),
        1,
        "finishing the retained chain must move it, not copy it",
    );
}

/// Both orderings matter: a finished proposal must be cleared when its parent leaves, and a
/// proposal still validating on the old snapshot must not put those chains back afterwards.
#[test]
fn publishing_a_changed_parent_releases_chains_even_when_proposals_stop() {
    let _init_guard = zakura_test::init();
    for finish_before_publication in [true, false] {
        for remove_all_chains in [false, true] {
            let (finalized, mut state, child, _, _) = state_with_spend();
            let parent = Arc::downgrade(state.best_chain().unwrap());
            let old_tip = state.best_tip();
            let mut proposal_state = state.clone();
            proposal_state.disable_metrics();
            proposal_state.record_proposal_results();

            if finish_before_publication {
                proposal_state
                    .commit_block(child.clone().prepare(), &finalized)
                    .expect("the proposal is valid on the original snapshot");
            }

            if remove_all_chains {
                let root = state.best_chain().unwrap().non_finalized_root_hash();
                state.invalidate_block(root).unwrap();
                assert!(state.is_chain_set_empty());
            } else {
                state.finalize();
                assert_eq!(state.best_tip(), old_tip, "finalization keeps the tip hash");
            }
            publish(&state);

            if !finish_before_publication {
                proposal_state
                    .commit_block(child.clone().prepare(), &finalized)
                    .expect("the in-flight proposal finishes on its original snapshot");
            }

            let prepared = Arc::downgrade(proposal_state.best_chain().unwrap());
            let spends = proposal_state
                .best_tip_block()
                .unwrap()
                .spent_outputs
                .clone();
            drop(proposal_state);

            // Do not submit a block or another proposal before checking release.
            assert!(
                parent.upgrade().is_none(),
                "the old parent is no longer pinned"
            );
            assert!(
                prepared.upgrade().is_none(),
                "the prepared chain is no longer pinned"
            );
            let entry = state.proposal_results.take(&child.prepare()).unwrap();
            assert!(Arc::ptr_eq(&entry.spent_utxos, &spends));
            assert_eq!(
                entry.prepared_proposal.err(),
                Some(NotPrepared::ParentChanged)
            );
        }
    }
}

#[test]
fn unpublished_changes_do_not_discard_a_reusable_proposal() {
    let _init_guard = zakura_test::init();
    let (finalized, state, child, _, _) = state_with_spend();
    let mut proposal_state = state.clone();
    proposal_state.disable_metrics();
    proposal_state.record_proposal_results();
    proposal_state
        .commit_block(child.clone().prepare(), &finalized)
        .unwrap();
    drop(proposal_state);

    let mut staged = state.clone();
    staged.finalize();
    drop(staged);
    publish(&state);

    let (spends, prepared) = state
        .proposal_results
        .take_for(&solve(&child).prepare(), state.best_chain());
    assert!(spends.is_some());
    assert!(
        prepared.is_ok(),
        "only accepted writer changes may discard a prepared chain"
    );
}

#[test]
fn cleanup_preserves_live_side_chains_and_distinct_miss_reasons() {
    let _init_guard = zakura_test::init();
    let (finalized, state, child, _, _) = state_with_spend();
    let (parent, prepared) = prepare(&state, &finalized, child.clone().prepare());
    let results = ProposalResults::default();
    let other_parent = Arc::new(parent.as_ref().clone());
    results.update_live_chains([&parent, &other_parent].into_iter());
    results.remember(
        prepared.tip_block().unwrap(),
        Some(PreparedProposal::new(&parent, &prepared)),
    );

    // Removing a different chain must leave this proposal reusable, even if it is a side chain.
    results.update_live_chains([&parent].into_iter());
    assert!(results
        .take_for(&solve(&child).prepare(), Some(&parent))
        .1
        .is_ok());

    results.remember(
        prepared.tip_block().unwrap(),
        Some(PreparedProposal::new(&parent, &prepared)),
    );
    // Equal contents and the same tip hash do not make a new allocation the same parent.
    results.update_live_chains([&other_parent].into_iter());
    for index in 0..MAX_PREPARED_CHAINS {
        let mut other = child.as_ref().clone();
        Arc::make_mut(&mut other.header).previous_block_hash =
            Hash([u8::try_from(index).unwrap(); 32]);
        results.remember(
            &contextual(
                Arc::new(other).prepare(),
                &prepared.tip_block().unwrap().spent_outputs,
            ),
            None,
        );
    }
    let (spends, reuse) = results.take_for(&solve(&child).prepare(), Some(&other_parent));
    assert!(spends.is_some());
    assert_eq!(
        reuse.err(),
        Some(NotPrepared::ParentChanged),
        "the memory bound must preserve the earlier cleanup reason"
    );
}
