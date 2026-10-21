//! Public, ordered chain inputs for wallet and sidecar proof construction.

use serde::{Deserialize, Serialize};
use zakura_chain::{block, tachyon, transaction};
use zcash_tachyon::{EpochIndex, TachyonBundle};

/// Response to `gettachyonblock`. Field-element and point encodings are canonical
/// Tachyon wire bytes in hex; block hashes and txids are hex strings in conventional display order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTachyonBlockResponse {
    /// Selected best-chain block hash, to persist as the client's resume cursor.
    #[serde(with = "hex")]
    pub hash: block::Hash,
    /// Previous block hash, for detecting forks and gaps.
    #[serde(with = "hex")]
    pub previous_block_hash: block::Hash,
    /// Absolute chain height.
    pub height: block::Height,
    /// NuTachyon activation height.
    pub activation_height: block::Height,
    /// Pool-relative block height, starting at zero on activation.
    pub pool_height: u32,
    /// Absolute Tachyon epoch index, starting at zero on activation.
    pub epoch: u32,
    /// Number of blocks per epoch in this node's protocol build.
    pub epoch_length: u32,
    /// Whether this block had reached finalized state when read.
    pub finalized: bool,
    /// Anchor before this block's epoch crossing and stamps. At activation this
    /// is already the epoch-zero entry anchor.
    #[serde(with = "hex")]
    pub anchor_before: [u8; 32],
    /// Entry anchor when this block starts an epoch, including epoch zero.
    /// Otherwise null. The crossing precedes every stamp in this block.
    pub epoch_start_anchor: Option<String>,
    /// Anchor after every stamp in this block.
    #[serde(with = "hex")]
    pub anchor_after: [u8; 32],
    /// Proof-bearing stamps in block transaction order. Adjunct/pointer bundles
    /// add no anchor step; their tachygrams appear in their aggregate's proof stamp.
    pub stamps: Vec<TachyonStampData>,
}

/// One stamp's contribution to the Tachyon anchor chain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TachyonStampData {
    /// Position in the block's complete transaction list, including coinbase.
    pub transaction_index: usize,
    /// Transaction containing the proof stamp.
    #[serde(with = "hex")]
    pub txid: transaction::Hash,
    /// Commitment absorbed by this stamp's anchor step.
    #[serde(with = "hex")]
    pub tachygram_set: [u8; 32],
    /// Canonically ordered tachygrams, encoded as canonical 32-byte hex strings.
    pub tachygrams: Vec<String>,
}

impl GetTachyonBlockResponse {
    pub(crate) fn from_state(data: zakura_state::TachyonBlock) -> Result<Self, String> {
        let pool_height = data
            .height
            .0
            .checked_sub(data.activation_height.0)
            .ok_or("NuTachyon is not active at the requested block")?;
        let epoch = tachyon::epoch_of_pool_height(pool_height);
        let mut anchor = zcash_tachyon::Anchor::read(&data.anchor_before.0[..])
            .map_err(|error| error.to_string())?;
        let epoch_start_anchor = if tachyon::is_epoch_first(pool_height) {
            if pool_height > 0 {
                anchor = anchor
                    .next_epoch(EpochIndex::new(epoch))
                    .map_err(|error| error.to_string())?;
            }
            Some(hex::encode(tachyon::Anchor::from(anchor).0))
        } else {
            None
        };

        let mut stamps = Vec::new();
        for (transaction_index, transaction) in data.block.transactions.iter().enumerate() {
            let Some(shielded) = transaction.tachyon_shielded_data() else {
                continue;
            };
            let TachyonBundle::Proven(bundle) = &shielded.0 else {
                continue;
            };
            let mut tachygram_set = [0; 32];
            bundle
                .stamp
                .tachygram_set
                .write(&mut tachygram_set[..])
                .map_err(|error| error.to_string())?;
            anchor = anchor
                .next_stamp(EpochIndex::new(epoch), &bundle.stamp.tachygram_set)
                .map_err(|error| error.to_string())?;
            stamps.push(TachyonStampData {
                transaction_index,
                txid: transaction.hash(),
                tachygram_set,
                tachygrams: bundle
                    .stamp
                    .tachygrams
                    .iter()
                    .map(|gram| hex::encode(tachyon::Tachygram::from(*gram).0))
                    .collect(),
            });
        }
        if tachyon::Anchor::from(anchor) != data.anchor_after {
            return Err("Tachyon block inputs do not reproduce the stored anchor".into());
        }

        Ok(Self {
            hash: data.block.hash(),
            previous_block_hash: data.block.header.previous_block_hash,
            height: data.height,
            activation_height: data.activation_height,
            pool_height,
            epoch,
            epoch_length: tachyon::EPOCH_LENGTH,
            finalized: data.finalized,
            anchor_before: data.anchor_before.0,
            epoch_start_anchor,
            anchor_after: data.anchor_after.0,
            stamps,
        })
    }
}

#[cfg(test)]
mod tests;
