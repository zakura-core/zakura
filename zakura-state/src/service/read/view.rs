//! Coherent best-chain reads across memory and finalized storage.

use std::sync::Arc;

use zakura_chain::{block, transaction::Transaction, transparent};

use crate::{
    response::{BestChainUnspentOutput, MinedTx},
    service::{finalized_state::ZakuraDbSnapshot, non_finalized_state::Chain},
    BoxError,
};

/// A best-chain view captured at one non-finalized generation and one RocksDB sequence.
pub(in crate::service) struct BestChainReadView<'a> {
    best_chain: Option<Arc<Chain>>,
    finalized: ZakuraDbSnapshot<'a>,
}

impl<'a> BestChainReadView<'a> {
    /// Creates a coherent best-chain view from captured state.
    pub(in crate::service) fn new(
        best_chain: Option<Arc<Chain>>,
        finalized: ZakuraDbSnapshot<'a>,
    ) -> Self {
        // A read-only secondary can advance its finalized snapshot before it
        // publishes the corresponding non-finalized state. Once finalized
        // storage reaches or passes the captured chain tip, it supersedes the
        // entire stale overlay. Keeping that overlay could resurrect outputs
        // already spent in the newer finalized view.
        let best_chain = match best_chain {
            Some(chain)
                if finalized.tip().is_some_and(|(finalized_height, _)| {
                    finalized_height >= chain.non_finalized_tip().0
                }) =>
            {
                None
            }
            best_chain => best_chain,
        };

        Self {
            best_chain,
            finalized,
        }
    }

    /// Returns the tip represented by this view.
    pub(in crate::service) fn tip(&self) -> Option<(block::Height, block::Hash)> {
        self.best_chain
            .as_ref()
            .map(|chain| chain.non_finalized_tip())
            .or_else(|| self.finalized.tip())
    }

    /// Returns an unspent output's transaction and tip context from this view.
    pub(in crate::service) fn unspent_output(
        &self,
        outpoint: transparent::OutPoint,
    ) -> Result<Option<BestChainUnspentOutput>, BoxError> {
        let chain = self.best_chain.as_ref();

        if chain.is_some_and(|chain| chain.spent_utxos.contains_key(&outpoint)) {
            return Ok(None);
        }

        let output_exists = chain
            .and_then(|chain| chain.created_utxo(&outpoint))
            .is_some()
            || self.finalized.contains_unspent_output(&outpoint);

        if !output_exists {
            return Ok(None);
        }

        let (tx, height, block_time): (Arc<Transaction>, _, _) = chain
            .and_then(|chain| {
                chain
                    .transaction(outpoint.hash)
                    .map(|(tx, height, time)| (tx.clone(), height, time))
            })
            .or_else(|| self.finalized.transaction(outpoint.hash))
            .ok_or_else(|| {
                // `gettxout` needs the creating transaction's version. A pruned raw
                // transaction cannot supply it, and returning `None` would falsely
                // report a known unspent output as absent.
                BoxError::from("creating transaction is unavailable in the captured state view")
            })?;
        let (tip_height, tip_hash) = self
            .tip()
            .ok_or_else(|| BoxError::from("coherent best-chain view has no tip"))?;
        let confirmations = tip_height
            .0
            .checked_sub(height.0)
            .ok_or_else(|| BoxError::from("transaction height is above the captured tip"))?
            .checked_add(1)
            .ok_or_else(|| BoxError::from("confirmation count exceeds u32"))?;

        Ok(Some(BestChainUnspentOutput {
            tip_hash,
            transaction: MinedTx::new(tx, height, confirmations, block_time),
        }))
    }
}
