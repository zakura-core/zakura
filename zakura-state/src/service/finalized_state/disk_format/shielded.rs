//! Shielded transfer serialization formats for finalized data.
//!
//! # Correctness
//!
//! [`crate::constants::state_database_format_version_in_code()`] must be incremented
//! each time the database format (column, serialization, etc) changes.

use bincode::Options;

use zakura_chain::{
    block::{merkle::AuthDataRoot, Height},
    ironwood, orchard, sapling, sprout,
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex},
};

use crate::service::finalized_state::disk_format::{FromDisk, IntoDisk};

use super::block::HEIGHT_DISK_BYTES;

impl IntoDisk for sprout::Nullifier {
    type Bytes = [u8; 32];

    fn as_bytes(&self) -> Self::Bytes {
        *self.0
    }
}

impl IntoDisk for sapling::Nullifier {
    type Bytes = [u8; 32];

    fn as_bytes(&self) -> Self::Bytes {
        *self.0
    }
}

impl IntoDisk for orchard::Nullifier {
    type Bytes = [u8; 32];

    fn as_bytes(&self) -> Self::Bytes {
        let nullifier: orchard::Nullifier = *self;
        nullifier.into()
    }
}

impl IntoDisk for sprout::tree::Root {
    type Bytes = [u8; 32];

    fn as_bytes(&self) -> Self::Bytes {
        self.into()
    }
}

impl FromDisk for sprout::tree::Root {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let array: [u8; 32] = bytes.as_ref().try_into().unwrap();
        array.into()
    }
}

impl IntoDisk for sapling::tree::Root {
    type Bytes = [u8; 32];

    fn as_bytes(&self) -> Self::Bytes {
        self.into()
    }
}

impl FromDisk for sapling::tree::Root {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let array: [u8; 32] = bytes.as_ref().try_into().unwrap();
        array.try_into().expect("finalized data must be valid")
    }
}

impl IntoDisk for orchard::tree::Root {
    type Bytes = [u8; 32];

    fn as_bytes(&self) -> Self::Bytes {
        self.into()
    }
}

impl FromDisk for orchard::tree::Root {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let array: [u8; 32] = bytes.as_ref().try_into().unwrap();
        array.try_into().expect("finalized data must be valid")
    }
}

/// The per-height Sapling and Orchard note-commitment roots, as stored in the
/// `commitment_roots_by_height` index (keyed by [`Height`]).
///
/// Every node persists this 64-byte value for each committed block — including a
/// verified-commitment-trees fast-synced node, which folds these roots in but writes no
/// per-height note-commitment trees. It lets such a node still serve the `tree_aux`
/// `BlockRoots` read from a compact index rather than from the (absent) trees.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitmentRootsByHeight {
    /// The Sapling note-commitment tree root at this height.
    pub sapling: sapling::tree::Root,
    /// The Orchard note-commitment tree root at this height.
    pub orchard: orchard::tree::Root,
    /// The ZIP-244 authorizing-data root (`hashAuthDataRoot`) of this block's
    /// transactions. Stored alongside the note-commitment roots so this node can
    /// serve it as the co-input needed to authenticate the *predecessor's* roots
    /// against this block's NU5+ header commitment, without re-reading the body.
    /// Default/zero below NU5.
    pub auth_data_root: AuthDataRoot,
    /// The Ironwood note-commitment tree root at this height (empty below NU6.3). Stored so a
    /// fast-synced node can serve it as a ZIP-221 V3 history-leaf input.
    pub ironwood: ironwood::tree::Root,
    /// This block's Sapling shielded transaction count — a ZIP-221 history-leaf input the
    /// header and roots don't provide, stored so it can be served for header-sync verification.
    pub sapling_tx: u64,
    /// This block's Orchard shielded transaction count (V2 leaf input, NU5+).
    pub orchard_tx: u64,
    /// This block's Ironwood shielded transaction count (V3 leaf input, NU6.3+).
    pub ironwood_tx: u64,
}

impl IntoDisk for CommitmentRootsByHeight {
    type Bytes = [u8; 152];

    fn as_bytes(&self) -> Self::Bytes {
        let mut out = [0u8; 152];
        out[..32].copy_from_slice(&IntoDisk::as_bytes(&self.sapling));
        out[32..64].copy_from_slice(&IntoDisk::as_bytes(&self.orchard));
        out[64..96].copy_from_slice(&<[u8; 32]>::from(self.auth_data_root));
        out[96..128].copy_from_slice(&IntoDisk::as_bytes(&self.ironwood));
        out[128..136].copy_from_slice(&self.sapling_tx.to_be_bytes());
        out[136..144].copy_from_slice(&self.orchard_tx.to_be_bytes());
        out[144..152].copy_from_slice(&self.ironwood_tx.to_be_bytes());
        out
    }
}

impl FromDisk for CommitmentRootsByHeight {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = bytes.as_ref();
        // Backwards compatible with shorter rows written by an earlier pre-release build of
        // this database version (64 bytes pre-auth-data, 96 bytes pre-ironwood/counts): the
        // missing fields decode as empty/zero, so the writer falls back to the body-wait path
        // for those heights until they are re-served with full data. New rows are 152 bytes.
        let auth_data_root = if bytes.len() >= 96 {
            let mut auth_data_root = [0u8; 32];
            auth_data_root.copy_from_slice(&bytes[64..96]);
            AuthDataRoot::from(auth_data_root)
        } else {
            AuthDataRoot::from([0u8; 32])
        };
        let (ironwood, sapling_tx, orchard_tx, ironwood_tx) = if bytes.len() >= 152 {
            (
                ironwood::tree::Root::from_bytes(&bytes[96..128]),
                u64::from_be_bytes(bytes[128..136].try_into().expect("8 bytes")),
                u64::from_be_bytes(bytes[136..144].try_into().expect("8 bytes")),
                u64::from_be_bytes(bytes[144..152].try_into().expect("8 bytes")),
            )
        } else {
            (
                ironwood::tree::NoteCommitmentTree::default().root(),
                0,
                0,
                0,
            )
        };
        CommitmentRootsByHeight {
            sapling: sapling::tree::Root::from_bytes(&bytes[..32]),
            orchard: orchard::tree::Root::from_bytes(&bytes[32..64]),
            auth_data_root,
            ironwood,
            sapling_tx,
            orchard_tx,
            ironwood_tx,
        }
    }
}

impl IntoDisk for NoteCommitmentSubtreeIndex {
    type Bytes = [u8; 2];

    fn as_bytes(&self) -> Self::Bytes {
        self.0.to_be_bytes()
    }
}

impl FromDisk for NoteCommitmentSubtreeIndex {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let array: [u8; 2] = bytes.as_ref().try_into().unwrap();
        Self(u16::from_be_bytes(array))
    }
}

// The following implementations for the note commitment trees use `serde` and
// `bincode`. `serde` serializations depend on the inner structure of the type.
// They should not be used in new code. (This is an issue for any derived serialization format.)
//
// We explicitly use `bincode::DefaultOptions`  to disallow trailing bytes; see
// https://docs.rs/bincode/1.3.3/bincode/config/index.html#options-struct-vs-bincode-functions

impl IntoDisk for sprout::tree::NoteCommitmentTree {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        bincode::DefaultOptions::new()
            .serialize(self)
            .expect("serialization to vec doesn't fail")
    }
}

impl FromDisk for sprout::tree::NoteCommitmentTree {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        bincode::DefaultOptions::new()
            .deserialize(bytes.as_ref())
            .expect("deserialization format should match the serialization format used by IntoDisk")
    }
}
impl IntoDisk for sapling::tree::NoteCommitmentTree {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        bincode::DefaultOptions::new()
            .serialize(self)
            .expect("serialization to vec doesn't fail")
    }
}

impl FromDisk for sapling::tree::NoteCommitmentTree {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        bincode::DefaultOptions::new()
            .deserialize(bytes.as_ref())
            .expect("deserialization format should match the serialization format used by IntoDisk")
    }
}

impl IntoDisk for orchard::tree::NoteCommitmentTree {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        bincode::DefaultOptions::new()
            .serialize(self)
            .expect("serialization to vec doesn't fail")
    }
}

impl FromDisk for orchard::tree::NoteCommitmentTree {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        bincode::DefaultOptions::new()
            .deserialize(bytes.as_ref())
            .expect("deserialization format should match the serialization format used by IntoDisk")
    }
}

impl IntoDisk for sapling_crypto::Node {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        self.to_bytes().to_vec()
    }
}

impl IntoDisk for orchard::tree::Node {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        self.to_repr().to_vec()
    }
}

impl<Root: IntoDisk<Bytes = Vec<u8>>> IntoDisk for NoteCommitmentSubtreeData<Root> {
    type Bytes = Vec<u8>;

    fn as_bytes(&self) -> Self::Bytes {
        [self.end_height.as_bytes().to_vec(), self.root.as_bytes()].concat()
    }
}

impl FromDisk for sapling_crypto::Node {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        Self::from_bytes(
            bytes
                .as_ref()
                .try_into()
                .expect("trusted data should be 32 bytes"),
        )
        .expect("trusted data should deserialize successfully")
    }
}

impl FromDisk for orchard::tree::Node {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        Self::try_from(bytes.as_ref()).expect("trusted data should deserialize successfully")
    }
}

impl<Node: FromDisk> FromDisk for NoteCommitmentSubtreeData<Node> {
    fn from_bytes(disk_bytes: impl AsRef<[u8]>) -> Self {
        let (height_bytes, node_bytes) = disk_bytes.as_ref().split_at(HEIGHT_DISK_BYTES);
        Self::new(
            Height::from_bytes(height_bytes),
            Node::from_bytes(node_bytes),
        )
    }
}
