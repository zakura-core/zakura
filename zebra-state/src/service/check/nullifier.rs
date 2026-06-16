//! Checks for nullifier uniqueness.

use std::{collections::HashMap, sync::Arc};

use tracing::trace;
use zebra_chain::{ironwood, transaction::Transaction};

use crate::{
    error::DuplicateNullifierError,
    service::{
        finalized_state::ZebraDb,
        non_finalized_state::{Chain, SpendingTransactionId},
    },
    SemanticallyVerifiedBlock, ValidateContextError,
};

// Tidy up some doc links
#[allow(unused_imports)]
use crate::service;

/// Reject double-spends of nullifers:
/// - one from this [`SemanticallyVerifiedBlock`], and the other already committed to the
///   [`FinalizedState`](service::FinalizedState).
///
/// (Duplicate non-finalized nullifiers are rejected during the chain update,
/// see [`add_to_non_finalized_chain_unique`] for details.)
///
/// # Consensus
///
/// > A nullifier MUST NOT repeat either within a transaction,
/// > or across transactions in a valid blockchain.
/// > Sprout, Sapling, Orchard, and Ironwood nullifiers are considered
/// > disjoint, even if they have the same bit pattern.
///
/// <https://zips.z.cash/protocol/protocol.pdf#nullifierset>
#[tracing::instrument(skip(semantically_verified, finalized_state))]
pub(crate) fn no_duplicates_in_finalized_chain(
    semantically_verified: &SemanticallyVerifiedBlock,
    finalized_state: &ZebraDb,
) -> Result<(), ValidateContextError> {
    for nullifier in semantically_verified.block.sprout_nullifiers() {
        if finalized_state.contains_sprout_nullifier(nullifier) {
            Err(nullifier.duplicate_nullifier_error(true))?;
        }
    }

    for nullifier in semantically_verified.block.sapling_nullifiers() {
        if finalized_state.contains_sapling_nullifier(nullifier) {
            Err(nullifier.duplicate_nullifier_error(true))?;
        }
    }

    for nullifier in semantically_verified.block.orchard_nullifiers() {
        if finalized_state.contains_orchard_nullifier(nullifier) {
            Err(nullifier.duplicate_nullifier_error(true))?;
        }
    }

    for nullifier in semantically_verified.block.ironwood_nullifiers() {
        if finalized_state.contains_ironwood_nullifier(nullifier) {
            Err(duplicate_ironwood_nullifier_error(nullifier, true))?;
        }
    }

    Ok(())
}

/// Accepts an iterator of revealed nullifiers, a predicate fn for checking if a nullifier is in
/// in the finalized chain, and a predicate fn for checking if the nullifier is in the non-finalized chain
///
/// Returns `Err(DuplicateNullifierError)` if any of the `revealed_nullifiers` are found in the
/// non-finalized or finalized chains.
///
/// Returns `Ok(())` if all the `revealed_nullifiers` have not been seen in either chain.
fn find_duplicate_nullifier_with<
    'a,
    NullifierT,
    FinalizedStateContainsFn,
    NonFinalizedStateContainsFn,
    DuplicateNullifierErrorFn,
>(
    revealed_nullifiers: impl IntoIterator<Item = &'a NullifierT>,
    finalized_chain_contains: FinalizedStateContainsFn,
    non_finalized_chain_contains: Option<NonFinalizedStateContainsFn>,
    duplicate_nullifier_error: DuplicateNullifierErrorFn,
) -> Result<(), ValidateContextError>
where
    NullifierT: 'a,
    FinalizedStateContainsFn: Fn(&'a NullifierT) -> bool,
    NonFinalizedStateContainsFn: Fn(&'a NullifierT) -> bool,
    DuplicateNullifierErrorFn: Fn(&NullifierT, bool) -> ValidateContextError,
{
    for nullifier in revealed_nullifiers {
        if let Some(true) = non_finalized_chain_contains.as_ref().map(|f| f(nullifier)) {
            Err(duplicate_nullifier_error(nullifier, false))?
        } else if finalized_chain_contains(nullifier) {
            Err(duplicate_nullifier_error(nullifier, true))?
        }
    }

    Ok(())
}

fn find_duplicate_nullifier<'a, NullifierT, FinalizedStateContainsFn, NonFinalizedStateContainsFn>(
    revealed_nullifiers: impl IntoIterator<Item = &'a NullifierT>,
    finalized_chain_contains: FinalizedStateContainsFn,
    non_finalized_chain_contains: Option<NonFinalizedStateContainsFn>,
) -> Result<(), ValidateContextError>
where
    NullifierT: DuplicateNullifierError + 'a,
    FinalizedStateContainsFn: Fn(&'a NullifierT) -> bool,
    NonFinalizedStateContainsFn: Fn(&'a NullifierT) -> bool,
{
    find_duplicate_nullifier_with(
        revealed_nullifiers,
        finalized_chain_contains,
        non_finalized_chain_contains,
        |nullifier, in_finalized_state| nullifier.duplicate_nullifier_error(in_finalized_state),
    )
}

fn duplicate_ironwood_nullifier_error(
    nullifier: &ironwood::Nullifier,
    in_finalized_state: bool,
) -> ValidateContextError {
    ValidateContextError::DuplicateIronwoodNullifier {
        nullifier: *nullifier,
        in_finalized_state,
    }
}

/// Reject double-spends of nullifiers:
/// - one from this [`Transaction`], and the other already committed to the
///   provided non-finalized [`Chain`] or [`ZebraDb`].
///
/// # Consensus
///
/// > A nullifier MUST NOT repeat either within a transaction,
/// > or across transactions in a valid blockchain.
/// > Sprout, Sapling, Orchard, and Ironwood nullifiers are considered
/// > disjoint, even if they have the same bit pattern.
///
/// <https://zips.z.cash/protocol/protocol.pdf#nullifierset>
#[tracing::instrument(skip_all)]
pub(crate) fn tx_no_duplicates_in_chain(
    finalized_chain: &ZebraDb,
    non_finalized_chain: Option<&Arc<Chain>>,
    transaction: &Arc<Transaction>,
) -> Result<(), ValidateContextError> {
    find_duplicate_nullifier(
        transaction.sprout_nullifiers(),
        |nullifier| finalized_chain.contains_sprout_nullifier(nullifier),
        non_finalized_chain
            .map(|chain| |nullifier| chain.sprout_nullifiers.contains_key(nullifier)),
    )?;

    find_duplicate_nullifier(
        transaction.sapling_nullifiers(),
        |nullifier| finalized_chain.contains_sapling_nullifier(nullifier),
        non_finalized_chain
            .map(|chain| |nullifier| chain.sapling_nullifiers.contains_key(nullifier)),
    )?;

    find_duplicate_nullifier(
        transaction.orchard_nullifiers(),
        |nullifier| finalized_chain.contains_orchard_nullifier(nullifier),
        non_finalized_chain
            .map(|chain| |nullifier| chain.orchard_nullifiers.contains_key(nullifier)),
    )?;

    find_duplicate_nullifier_with(
        transaction.ironwood_nullifiers(),
        |nullifier| finalized_chain.contains_ironwood_nullifier(nullifier),
        non_finalized_chain
            .map(|chain| |nullifier| chain.ironwood_nullifiers.contains_key(nullifier)),
        duplicate_ironwood_nullifier_error,
    )?;

    Ok(())
}

/// Reject double-spends of nullifers:
/// - both within the same `JoinSplit` (sprout only),
/// - from different `JoinSplit`s, [`sapling::Spend`][2]s,
///   [`orchard::Action`][3]s, or Ironwood actions in this
///   [`Transaction`]'s shielded data, or
/// - one from this shielded data, and another from:
///   - a previous transaction in this [`Block`][4], or
///   - a previous block in this non-finalized [`Chain`].
///
/// (Duplicate finalized nullifiers are rejected during service contextual validation,
/// see [`no_duplicates_in_finalized_chain`] for details.)
///
/// # Consensus
///
/// > A nullifier MUST NOT repeat either within a transaction,
/// > or across transactions in a valid blockchain.
/// > Sprout, Sapling, Orchard, and Ironwood nullifiers are considered
/// > disjoint, even if they have the same bit pattern.
///
/// <https://zips.z.cash/protocol/protocol.pdf#nullifierset>
///
/// We comply with the "disjoint" rule by storing the nullifiers for each
/// pool in separate sets, so that even if different pools have nullifiers
/// with same bit pattern, they won't be considered the same when determining
/// uniqueness. This is enforced by the callers of this function.
///
/// [2]: zebra_chain::sapling::Spend
/// [3]: zebra_chain::orchard::Action
/// [4]: zebra_chain::block::Block
fn add_to_non_finalized_chain_unique_with<'block, NullifierT, DuplicateNullifierErrorFn>(
    chain_nullifiers: &mut HashMap<NullifierT, SpendingTransactionId>,
    shielded_data_nullifiers: impl IntoIterator<Item = &'block NullifierT>,
    revealing_tx_id: SpendingTransactionId,
    duplicate_nullifier_error: DuplicateNullifierErrorFn,
) -> Result<(), ValidateContextError>
where
    NullifierT: Copy + std::fmt::Debug + Eq + std::hash::Hash + 'block,
    DuplicateNullifierErrorFn: Fn(&NullifierT, bool) -> ValidateContextError,
{
    for nullifier in shielded_data_nullifiers.into_iter() {
        trace!(?nullifier, "adding nullifier");

        // reject the nullifier if it is already present in this non-finalized chain
        if chain_nullifiers
            .insert(*nullifier, revealing_tx_id)
            .is_some()
        {
            Err(duplicate_nullifier_error(nullifier, false))?;
        }
    }

    Ok(())
}

#[tracing::instrument(skip(chain_nullifiers, shielded_data_nullifiers))]
pub(crate) fn add_to_non_finalized_chain_unique<'block, NullifierT>(
    chain_nullifiers: &mut HashMap<NullifierT, SpendingTransactionId>,
    shielded_data_nullifiers: impl IntoIterator<Item = &'block NullifierT>,
    revealing_tx_id: SpendingTransactionId,
) -> Result<(), ValidateContextError>
where
    NullifierT: DuplicateNullifierError + Copy + std::fmt::Debug + Eq + std::hash::Hash + 'block,
{
    add_to_non_finalized_chain_unique_with(
        chain_nullifiers,
        shielded_data_nullifiers,
        revealing_tx_id,
        |nullifier, in_finalized_state| nullifier.duplicate_nullifier_error(in_finalized_state),
    )
}

#[tracing::instrument(skip(chain_nullifiers, shielded_data_nullifiers))]
pub(crate) fn add_ironwood_to_non_finalized_chain_unique<'block>(
    chain_nullifiers: &mut HashMap<ironwood::Nullifier, SpendingTransactionId>,
    shielded_data_nullifiers: impl IntoIterator<Item = &'block ironwood::Nullifier>,
    revealing_tx_id: SpendingTransactionId,
) -> Result<(), ValidateContextError> {
    add_to_non_finalized_chain_unique_with(
        chain_nullifiers,
        shielded_data_nullifiers,
        revealing_tx_id,
        duplicate_ironwood_nullifier_error,
    )
}

/// Remove nullifiers that were previously added to this non-finalized
/// [`Chain`][1] by this shielded data.
///
/// "A note can change from being unspent to spent as a node’s view
/// of the best valid block chain is extended by new transactions.
///
/// Also, block chain reorganizations can cause a node to switch
/// to a different best valid block chain that does not contain
/// the transaction in which a note was output"
///
/// <https://zips.z.cash/protocol/nu5.pdf#decryptivk>
///
/// Note: reorganizations can also change the best chain to one
/// where a note was unspent, rather than spent.
///
/// # Panics
///
/// Panics if any nullifier is missing from the chain when we try to remove it.
///
/// Blocks with duplicate nullifiers are rejected by
/// [`add_to_non_finalized_chain_unique`], so this shielded data should be the
/// only shielded data that added this nullifier to this [`Chain`][1].
///
/// [1]: service::non_finalized_state::Chain
#[tracing::instrument(skip(chain_nullifiers, shielded_data_nullifiers))]
pub(crate) fn remove_from_non_finalized_chain<'block, NullifierT>(
    chain_nullifiers: &mut HashMap<NullifierT, SpendingTransactionId>,
    shielded_data_nullifiers: impl IntoIterator<Item = &'block NullifierT>,
) where
    NullifierT: std::fmt::Debug + Eq + std::hash::Hash + 'block,
{
    for nullifier in shielded_data_nullifiers.into_iter() {
        trace!(?nullifier, "removing nullifier");

        assert!(
            chain_nullifiers.remove(nullifier).is_some(),
            "nullifier must be present if block was added to chain"
        );
    }
}
