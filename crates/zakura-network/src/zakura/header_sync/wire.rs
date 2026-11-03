//! Header-sync wire types shared by every message: stream constants, header
//! entries, and auxiliary schema-1 records. The version 9 codec is in `v9`.

use std::{io, sync::Arc};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use thiserror::Error;
use zakura_chain::{
    block::{self, merkle::AuthDataRoot},
    ironwood, orchard,
    parameters::{Network, NetworkUpgrade},
    sapling,
    serialization::{SerializationError, ZcashDeserialize, ZcashSerialize},
};

/// Zakura stream kind reserved for native header sync.
pub const ZAKURA_STREAM_HEADER_SYNC: u16 = 5;
/// The sole supported header-sync stream version.
///
/// The stream version provides a transport compatibility barrier.
/// The stream version does not select the codec.
pub const ZAKURA_HEADER_SYNC_STREAM_VERSION: u16 = 9;
/// Maximum encoded header-sync message bytes.
pub const MAX_HS_MESSAGE_BYTES: usize = 2 * 1024 * 1024;
/// Default number of headers advertised per response.
pub const DEFAULT_HS_RANGE: u32 = 1000;
/// Maximum number of headers accepted in one response.
pub const MAX_HS_RANGE: u32 = 4000;

pub(super) const KNOWN_TREE_AUX_SCHEMA_MASK: u32 = 1;
/// Exact encoded length of one immutable tree-aux schema-1 record.
pub const TREE_AUX_SCHEMA_V1_BYTES: usize = 4 + 32 + 32 + 32 + 8 + 8 + 8 + 32;

/// Errors produced while encoding, decoding, or checking schema-1 records.
#[derive(Debug, Error)]
pub enum HeaderSyncWireError {
    /// A height exceeded the locally supported block-height range.
    #[error("Zakura header-sync height {0} exceeds the supported range")]
    HeightOutOfRange(u32),
    /// A tree-aux record had the wrong inferred height.
    #[error(
        "Zakura header-sync tree-aux height {actual:?} does not match inferred height {expected:?}"
    )]
    TreeAuxHeightMismatch {
        /// Height inferred from the common ancestor and record offset.
        expected: block::Height,
        /// Height encoded in the record.
        actual: block::Height,
    },
    /// A schema-1 record violated activation-dependent defaults.
    #[error("invalid Zakura header-sync tree-aux defaults at height {height:?}: {field}")]
    InvalidTreeAuxDefault {
        /// Record height.
        height: block::Height,
        /// Field that violated its activation-dependent default.
        field: &'static str,
    },
    /// An I/O error occurred while handling the message.
    #[error("Zakura header-sync wire I/O error: {0}")]
    Io(#[from] io::Error),
    /// A canonical Zcash type failed to serialize or deserialize.
    #[error("Zakura header-sync Zcash serialization error: {0}")]
    Serialization(#[from] SerializationError),
}

/// Immutable tree-aux selector values understood by this implementation.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum AuxSchema {
    /// The request asks for no tree auxiliary records.
    /// The response returns no tree auxiliary records.
    #[default]
    None = 0,
    /// The immutable 156-byte schema defined by protocol version 8.
    V1 = 1,
}

impl AuxSchema {
    pub(crate) fn mask_bit(self) -> u32 {
        match self {
            Self::None => 0,
            Self::V1 => 1,
        }
    }

    pub(super) fn wire_value(self) -> u8 {
        match self {
            Self::None => 0,
            Self::V1 => 1,
        }
    }
}

/// One header with its parallel advisory metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderEntry {
    /// Canonical Zcash block header.
    pub header: Arc<block::Header>,
    /// Unauthenticated serialized-body-size hint.
    /// Zero means unknown.
    pub body_size: u32,
    /// Parallel schema-1 record when the response selects schema 1.
    pub tree_aux: Option<TreeAuxRecordV1>,
}

pub use zakura_header_chain::TreeAuxRecordV1;

pub(super) trait TreeAuxWire: Sized {
    /// Validate the inferred height and all activation-dependent canonical defaults.
    fn validate_for(
        &self,
        expected_height: block::Height,
        network: &Network,
    ) -> Result<(), HeaderSyncWireError>;

    fn encode_to<W: io::Write>(&self, writer: &mut W) -> Result<(), HeaderSyncWireError>;

    fn decode_from<R: io::Read>(reader: &mut R) -> Result<Self, HeaderSyncWireError>;
}

impl TreeAuxWire for TreeAuxRecordV1 {
    fn validate_for(
        &self,
        expected_height: block::Height,
        network: &Network,
    ) -> Result<(), HeaderSyncWireError> {
        if self.height != expected_height {
            return Err(HeaderSyncWireError::TreeAuxHeightMismatch {
                expected: expected_height,
                actual: self.height,
            });
        }

        if NetworkUpgrade::Nu5
            .activation_height(network)
            .is_none_or(|height| expected_height < height)
        {
            if self.orchard_root != orchard::tree::NoteCommitmentTree::default().root() {
                return Err(HeaderSyncWireError::InvalidTreeAuxDefault {
                    height: expected_height,
                    field: "orchard_root",
                });
            }
            if self.orchard_tx_count != 0 {
                return Err(HeaderSyncWireError::InvalidTreeAuxDefault {
                    height: expected_height,
                    field: "orchard_tx_count",
                });
            }
            if self.auth_data_root != AuthDataRoot::from([0; 32]) {
                return Err(HeaderSyncWireError::InvalidTreeAuxDefault {
                    height: expected_height,
                    field: "auth_data_root",
                });
            }
        }

        if NetworkUpgrade::Nu6_3
            .activation_height(network)
            .is_none_or(|height| expected_height < height)
        {
            if self.ironwood_root != ironwood::tree::NoteCommitmentTree::default().root() {
                return Err(HeaderSyncWireError::InvalidTreeAuxDefault {
                    height: expected_height,
                    field: "ironwood_root",
                });
            }
            if self.ironwood_tx_count != 0 {
                return Err(HeaderSyncWireError::InvalidTreeAuxDefault {
                    height: expected_height,
                    field: "ironwood_tx_count",
                });
            }
        }
        Ok(())
    }

    fn encode_to<W: io::Write>(&self, writer: &mut W) -> Result<(), HeaderSyncWireError> {
        writer.write_u32::<LittleEndian>(self.height.0)?;
        self.sapling_root.zcash_serialize(&mut *writer)?;
        self.orchard_root.zcash_serialize(&mut *writer)?;
        self.ironwood_root.zcash_serialize(&mut *writer)?;
        writer.write_u64::<LittleEndian>(self.sapling_tx_count)?;
        writer.write_u64::<LittleEndian>(self.orchard_tx_count)?;
        writer.write_u64::<LittleEndian>(self.ironwood_tx_count)?;
        writer.write_all(&<[u8; 32]>::from(self.auth_data_root))?;
        Ok(())
    }

    fn decode_from<R: io::Read>(reader: &mut R) -> Result<Self, HeaderSyncWireError> {
        let height = read_height(reader)?;
        let sapling_root = sapling::tree::Root::zcash_deserialize(&mut *reader)?;
        let orchard_root = orchard::tree::Root::zcash_deserialize(&mut *reader)?;
        let ironwood_root = ironwood::tree::Root::zcash_deserialize(&mut *reader)?;
        let sapling_tx_count = reader.read_u64::<LittleEndian>()?;
        let orchard_tx_count = reader.read_u64::<LittleEndian>()?;
        let ironwood_tx_count = reader.read_u64::<LittleEndian>()?;
        let mut auth_data_root = [0; 32];
        reader.read_exact(&mut auth_data_root)?;
        Ok(Self {
            height,
            sapling_root,
            orchard_root,
            ironwood_root,
            sapling_tx_count,
            orchard_tx_count,
            ironwood_tx_count,
            auth_data_root: AuthDataRoot::from(auth_data_root),
        })
    }
}

fn read_height<R: io::Read>(reader: &mut R) -> Result<block::Height, HeaderSyncWireError> {
    let height = block::Height(reader.read_u32::<LittleEndian>()?);
    if height > block::Height::MAX {
        return Err(HeaderSyncWireError::HeightOutOfRange(height.0));
    }
    Ok(height)
}

const _: () = assert!(TREE_AUX_SCHEMA_V1_BYTES == 156);
const _: () = assert!(KNOWN_TREE_AUX_SCHEMA_MASK == 1);
