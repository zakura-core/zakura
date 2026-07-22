//! A tonic RPC server for Zebra's indexer API.

use std::sync::Arc;

use zakura_chain::{
    block,
    serialization::{BytesInDisplayOrder, ZcashDeserializeInto, ZcashSerialize},
};

#[cfg(test)]
mod tests;

pub mod methods;
pub mod server;

/// The byte length of a block hash in indexer RPC requests.
const BLOCK_HASH_BYTE_LEN: usize = 32;

/// The byte length of a big-endian block height in indexer RPC requests.
const BLOCK_HEIGHT_BYTE_LEN: usize = std::mem::size_of::<u32>();

// The generated indexer proto
tonic::include_proto!("zebra.indexer.rpc");

pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("indexer_descriptor");

impl BlockHashAndHeight {
    /// Create a new [`BlockHashAndHeight`] from a [`block::Hash`] and [`block::Height`].
    pub fn new(hash: block::Hash, block::Height(height): block::Height) -> Self {
        let hash = hash.bytes_in_display_order().to_vec();
        BlockHashAndHeight { hash, height }
    }

    /// Try to convert a [`BlockHashAndHeight`] into a tuple of a block hash and height.
    pub fn try_into_hash_and_height(self) -> Option<(block::Hash, block::Height)> {
        self.hash
            .try_into()
            .map(|bytes| block::Hash::from_bytes_in_display_order(&bytes))
            .map_err(|bytes: Vec<_>| {
                tracing::warn!(
                    "failed to convert BlockHash to Hash, unexpected len: {}",
                    bytes.len()
                )
            })
            .ok()
            .zip(self.height.try_into().ok())
    }
}

impl BlockAndHash {
    /// Creates a new [`BlockAndHash`] from a [`block::Hash`] and [`block::Height`].
    ///
    /// # Panics
    ///
    /// This function will panic if the block serialization fails (if the header version is invalid).
    pub fn new(hash: block::Hash, block: Arc<block::Block>) -> Self {
        BlockAndHash {
            hash: hash.bytes_in_display_order().to_vec(),
            data: block
                .zcash_serialize_to_vec()
                .expect("block serialization should not fail"),
        }
    }

    /// Try to convert a [`BlockAndHash`] into a tuple of a decoded block and hash.
    ///
    /// Returns `None` if the advertised hash does not match the decoded block.
    pub fn decode(self) -> Option<(block::Block, block::Hash)> {
        let advertised_hash = self
            .hash
            .try_into()
            .map(|bytes| block::Hash::from_bytes_in_display_order(&bytes))
            .map_err(|bytes: Vec<_>| {
                tracing::warn!(
                    "failed to convert BlockHash to Hash, unexpected len: {}",
                    bytes.len()
                )
            })
            .ok()?;
        let block: block::Block = self
            .data
            .zcash_deserialize_into()
            .map_err(|err| tracing::warn!(?err, "failed to deserialize block"))
            .ok()?;
        let computed_hash = block.hash();

        if advertised_hash != computed_hash {
            tracing::warn!(
                ?advertised_hash,
                ?computed_hash,
                "advertised hash does not match decoded block"
            );
            return None;
        }

        Some((block, advertised_hash))
    }
}
