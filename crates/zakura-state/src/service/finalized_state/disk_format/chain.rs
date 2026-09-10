//! Chain data serialization formats for finalized data.
//!
//! # Correctness
//!
//! [`crate::constants::state_database_format_version_in_code()`] must be incremented
//! each time the database format (column, serialization, etc) changes.

use std::collections::BTreeMap;

use bincode::Options;
use serde_big_array::BigArray;
use thiserror::Error;

use zakura_chain::{
    amount::NonNegative,
    block::Height,
    block_info::BlockInfo,
    history_tree::{HistoryTreeError, NonEmptyHistoryTree},
    parameters::{Network, NetworkKind},
    primitives::zcash_history,
    value_balance::ValueBalance,
};

use crate::service::finalized_state::disk_format::{FromDisk, IntoDisk};

/// An error decoding a persisted history-tree snapshot.
#[derive(Debug, Error)]
pub enum HistoryTreeDecodeError {
    /// The snapshot does not use either supported serialization format.
    #[error(
        "history tree snapshot is neither the current nor legacy format: \
         current format error: {current}; legacy format error: {legacy}"
    )]
    InvalidEncoding {
        /// The current-width decoding error.
        current: bincode::Error,
        /// The legacy-width decoding error.
        legacy: bincode::Error,
    },

    /// The snapshot was created for a different network kind.
    #[error(
        "history tree snapshot network kind {stored:?} does not match configured network kind {configured:?}"
    )]
    NetworkMismatch {
        /// The network kind stored in the snapshot.
        stored: NetworkKind,
        /// The configured network kind.
        configured: NetworkKind,
    },

    /// The decoded snapshot does not contain a valid history tree.
    #[error("invalid history tree snapshot: {0}")]
    HistoryTree(#[from] HistoryTreeError),
}

impl IntoDisk for ValueBalance<NonNegative> {
    type Bytes = [u8; 48];

    fn as_bytes(&self) -> Self::Bytes {
        self.to_bytes()
    }
}

impl FromDisk for ValueBalance<NonNegative> {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        ValueBalance::from_bytes(bytes.as_ref()).expect("ValueBalance should be parsable")
    }
}

// The following implementations for history trees use `serde` and
// `bincode`. `serde` serializations depend on the inner structure of the type.
// They should not be used in new code. (This is an issue for any derived serialization format.)
//
// We explicitly use `bincode::DefaultOptions`  to disallow trailing bytes; see
// https://docs.rs/bincode/1.3.3/bincode/config/index.html#options-struct-vs-bincode-functions

#[derive(serde::Serialize, serde::Deserialize)]
pub struct HistoryTreeParts {
    network_kind: NetworkKind,
    size: u32,
    peaks: BTreeMap<u32, zcash_history::Entry>,
    current_height: Height,
}

impl HistoryTreeParts {
    /// Decodes current-width or legacy-width history-tree snapshot bytes.
    pub(crate) fn try_from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self, HistoryTreeDecodeError> {
        let bytes = bytes.as_ref();
        let options = bincode::DefaultOptions::new();

        // Try the current entry width first. Databases written before NU6.3 widened
        // `zcash_history::Entry` store narrower entries that fail to parse at the current width,
        // so fall back to the legacy width and zero-pad each entry up to the current width.
        //
        // Legacy-width rows can fail the current-width decoder with errors other than
        // `UnexpectedEof`, because the wider entry can read into the next legacy entry and
        // interpret arbitrary entry bytes as bincode control bytes.
        match options.deserialize::<HistoryTreeParts>(bytes) {
            Ok(parts) => Ok(parts),
            Err(current) => options
                .deserialize::<LegacyHistoryTreeParts>(bytes)
                .map(HistoryTreeParts::from)
                .map_err(|legacy| HistoryTreeDecodeError::InvalidEncoding { current, legacy }),
        }
    }

    /// Converts [`HistoryTreeParts`] to a [`NonEmptyHistoryTree`].
    pub(crate) fn with_network(
        self,
        network: &Network,
    ) -> Result<NonEmptyHistoryTree, HistoryTreeDecodeError> {
        let configured = network.kind();
        if self.network_kind != configured {
            return Err(HistoryTreeDecodeError::NetworkMismatch {
                stored: self.network_kind,
                configured,
            });
        }

        Ok(NonEmptyHistoryTree::from_cache(
            network,
            self.size,
            self.peaks,
            self.current_height,
        )?)
    }
}

impl From<&NonEmptyHistoryTree> for HistoryTreeParts {
    fn from(history_tree: &NonEmptyHistoryTree) -> Self {
        HistoryTreeParts {
            network_kind: history_tree.network().kind(),
            size: history_tree.size(),
            peaks: history_tree.peaks().clone(),
            current_height: history_tree.current_height(),
        }
    }
}

impl IntoDisk for HistoryTreeParts {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        bincode::DefaultOptions::new()
            .serialize(self)
            .expect("serialization to vec doesn't fail")
    }
}

/// The width of a history-tree [`zcash_history::Entry`] as serialized by database formats written
/// before NU6.3 widened `zcash_history::NodeData`.
const LEGACY_MAX_ENTRY_SIZE: usize = 253;

/// A mirror of [`HistoryTreeParts`] using the pre-NU6.3 [`zcash_history::Entry`] width.
#[derive(serde::Serialize, serde::Deserialize)]
struct LegacyHistoryTreeParts {
    network_kind: NetworkKind,
    size: u32,
    peaks: BTreeMap<u32, LegacyEntry>,
    current_height: Height,
}

/// A history-tree entry serialized at the pre-NU6.3 [`LEGACY_MAX_ENTRY_SIZE`] width.
#[derive(serde::Serialize, serde::Deserialize)]
struct LegacyEntry {
    #[serde(with = "BigArray")]
    inner: [u8; LEGACY_MAX_ENTRY_SIZE],
}

impl From<LegacyHistoryTreeParts> for HistoryTreeParts {
    fn from(legacy: LegacyHistoryTreeParts) -> Self {
        HistoryTreeParts {
            network_kind: legacy.network_kind,
            size: legacy.size,
            peaks: legacy
                .peaks
                .into_iter()
                .map(|(index, entry)| {
                    (
                        index,
                        zcash_history::Entry::from_raw_bytes_padded(&entry.inner),
                    )
                })
                .collect(),
            current_height: legacy.current_height,
        }
    }
}

impl FromDisk for HistoryTreeParts {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        HistoryTreeParts::try_from_bytes(bytes)
            .expect("deserialization format should match the serialization format used by IntoDisk")
    }
}

impl IntoDisk for BlockInfo {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        self.value_pools()
            .as_bytes()
            .iter()
            .copied()
            .chain(self.size().to_le_bytes().iter().copied())
            .collect()
    }
}

impl FromDisk for BlockInfo {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        // We have two different DB formats, one from NU6_1 (Lockbox) one for Ironwood and onwards.
        const NU6_1_VALUE_BALANCE_LEN: usize = 40;
        const IRONWOOD_VALUE_BALANCE_LEN: usize = 48;
        const BLOCK_SIZE_LEN: usize = 4;
        const NU6_1_BLOCK_INFO_LEN: usize = NU6_1_VALUE_BALANCE_LEN + BLOCK_SIZE_LEN;
        const IRONWOOD_BLOCK_INFO_LEN: usize = IRONWOOD_VALUE_BALANCE_LEN + BLOCK_SIZE_LEN;

        let bytes = bytes.as_ref();

        // We want to be forward-compatible, so this must work even if the
        // size of the buffer is larger than expected.
        match bytes.len() {
            IRONWOOD_BLOCK_INFO_LEN.. => {
                let value_pools =
                    ValueBalance::<NonNegative>::from_bytes(&bytes[..IRONWOOD_VALUE_BALANCE_LEN])
                        .expect("must work for 48 bytes");
                let size = u32::from_le_bytes(
                    bytes[IRONWOOD_VALUE_BALANCE_LEN..IRONWOOD_VALUE_BALANCE_LEN + BLOCK_SIZE_LEN]
                        .try_into()
                        .expect("must be 4 bytes"),
                );
                BlockInfo::new(value_pools, size)
            }
            NU6_1_BLOCK_INFO_LEN.. => {
                let value_pools =
                    ValueBalance::<NonNegative>::from_bytes(&bytes[..NU6_1_VALUE_BALANCE_LEN])
                        .expect("must work for 40 bytes");
                let size = u32::from_le_bytes(
                    bytes[NU6_1_VALUE_BALANCE_LEN..NU6_1_VALUE_BALANCE_LEN + BLOCK_SIZE_LEN]
                        .try_into()
                        .expect("must be 4 bytes"),
                );
                BlockInfo::new(value_pools, size)
            }
            _ => panic!("invalid format"),
        }
    }
}

#[cfg(test)]
mod tests;
