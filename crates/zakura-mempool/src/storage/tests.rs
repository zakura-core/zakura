//! Tests for mempool storage.

use zakura_chain::transaction::{self, UnminedTxId};
use zakura_rt::collections::HashSet;

use super::{verified_set::VerifiedSet, SameEffectsChainRejectionError};

mod eviction;
mod known_ids;
mod mutation_benchmarks;
mod prop;
mod standardness;
mod vectors;

/// The verifier's error, as these tests see it.
#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
#[error("the test verifier rejected the transaction")]
pub struct TestVerifyError;

/// Storage that caches [`TestVerifyError`].
pub type Storage = super::Storage<TestVerifyError>;

/// A mempool error that can hold [`TestVerifyError`].
pub type MempoolError = crate::MempoolError<TestVerifyError>;

/// An exact-tip rejection that can hold [`TestVerifyError`].
pub type ExactTipRejectionError = super::ExactTipRejectionError<TestVerifyError>;

/// A rejection that can hold [`TestVerifyError`].
pub type RejectionError = super::RejectionError<TestVerifyError>;

impl<E: Clone> super::Storage<E> {
    /// Removes the stored transactions that exactly match `exact_wtxids`, and
    /// what depends on them. Caches no rejection. Returns how many were
    /// removed.
    pub fn remove_exact(&mut self, exact_wtxids: &HashSet<UnminedTxId>) -> usize {
        let tx_ids: Vec<_> = exact_wtxids
            .iter()
            .filter(|id| {
                self.transactions()
                    .get(&id.mined_id())
                    .is_some_and(|tx| tx.transaction.id() == **id)
            })
            .map(UnminedTxId::mined_id)
            .collect();
        self.verified.remove_all(tx_ids).len()
    }

    /// Does to storage what the pool core does for a block that mined
    /// `mined_ids`: removes each one that is stored, keeping what depends on
    /// it, and caches each one as mined at `now`. Returns what was removed.
    pub fn mine(
        &mut self,
        mined_ids: &HashSet<transaction::Hash>,
        now: crate::Instant,
    ) -> HashSet<UnminedTxId> {
        let mut removed = HashSet::new();
        for mined_id in mined_ids {
            if let Some(tx) = self.remove_one(mined_id) {
                removed.insert(tx.transaction.id());
            }
            self.reject_effects_chain(*mined_id, SameEffectsChainRejectionError::Mined, now);
        }
        removed
    }
}

impl VerifiedSet {
    /// Removes the listed transactions and their descendants.
    ///
    /// Returns all removed unmined IDs. Missing IDs, duplicate IDs, and IDs
    /// already removed as descendants are ignored.
    pub(crate) fn remove_all(
        &mut self,
        tx_ids: impl IntoIterator<Item = transaction::Hash>,
    ) -> HashSet<UnminedTxId> {
        let mut removed_transactions = HashSet::new();

        for key_to_remove in tx_ids {
            if !self.contains(&key_to_remove) {
                // A previous removal may already have removed this descendant.
                continue;
            }

            removed_transactions.extend(
                self.remove(&key_to_remove)
                    .into_iter()
                    .map(|tx| tx.transaction.id()),
            );
        }

        removed_transactions
    }
}
