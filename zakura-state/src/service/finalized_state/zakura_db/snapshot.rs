//! Immutable reads from one RocksDB sequence.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use zakura_chain::{
    block::{self, Height},
    transaction::{self, Transaction},
    transparent,
};

use crate::service::finalized_state::{
    disk_db::DiskDbSnapshot,
    disk_format::{OutputLocation, TransactionLocation},
};

use super::ZakuraDb;

/// A coherent immutable view of the finalized state.
pub(in crate::service) struct ZakuraDbSnapshot<'a> {
    db: DiskDbSnapshot<'a>,
}

impl ZakuraDb {
    /// Captures a coherent immutable view of the finalized state.
    pub(in crate::service) fn snapshot(&self) -> ZakuraDbSnapshot<'_> {
        ZakuraDbSnapshot {
            db: self.db.snapshot(),
        }
    }
}

impl ZakuraDbSnapshot<'_> {
    /// Returns the finalized tip captured by this snapshot.
    pub(in crate::service) fn tip(&self) -> Option<(block::Height, block::Hash)> {
        let hash_by_height = self.db.cf_handle("hash_by_height")?;
        self.db.zs_last_key_value(&hash_by_height)
    }

    /// Returns the transaction and its mined context captured by this snapshot.
    pub(in crate::service) fn transaction(
        &self,
        hash: transaction::Hash,
    ) -> Option<(Arc<Transaction>, Height, DateTime<Utc>)> {
        let transaction_location = self.transaction_location(hash)?;
        let block_header_by_height = self.db.cf_handle("block_header_by_height")?;
        let header: Arc<block::Header> = self
            .db
            .zs_get(&block_header_by_height, &transaction_location.height)?;
        let tx_by_loc = self.db.cf_handle("tx_by_loc")?;
        let transaction = self.db.zs_get(&tx_by_loc, &transaction_location)?;

        Some((transaction, transaction_location.height, header.time))
    }

    /// Returns true if `outpoint` is unspent in this snapshot.
    pub(in crate::service) fn contains_unspent_output(
        &self,
        outpoint: &transparent::OutPoint,
    ) -> bool {
        let Some(transaction_location) = self.transaction_location(outpoint.hash) else {
            return false;
        };
        let output_location = OutputLocation::from_outpoint(transaction_location, outpoint);
        let Some(utxo_by_out_loc) = self.db.cf_handle("utxo_by_out_loc") else {
            return false;
        };
        let output: Option<transparent::Output> =
            self.db.zs_get(&utxo_by_out_loc, &output_location);

        output.is_some()
    }

    fn transaction_location(&self, hash: transaction::Hash) -> Option<TransactionLocation> {
        let tx_loc_by_hash = self.db.cf_handle("tx_loc_by_hash")?;
        self.db.zs_get(&tx_loc_by_hash, &hash)
    }
}
