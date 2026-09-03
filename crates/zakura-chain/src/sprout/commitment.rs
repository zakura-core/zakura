//! Sprout commitment types.

use crate::fmt::HexDebug;

/// The randomness used in the Pedersen Hash for note commitment.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(
    any(test, feature = "proptest-impl"),
    derive(proptest_derive::Arbitrary)
)]
pub struct CommitmentRandomness(pub HexDebug<[u8; 32]>);

impl AsRef<[u8]> for CommitmentRandomness {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

/// Note commitments for the output notes.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[cfg_attr(
    any(test, feature = "proptest-impl"),
    derive(proptest_derive::Arbitrary)
)]
pub struct NoteCommitment(pub(crate) HexDebug<[u8; 32]>);

impl NoteCommitment {
    /// Return the bytes in big-endian byte order as required
    /// by RPCs such as `getrawtransaction`.
    pub fn bytes_in_display_order(&self) -> [u8; 32] {
        let mut root: [u8; 32] = self.into();
        root.reverse();
        root
    }
}

impl From<[u8; 32]> for NoteCommitment {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes.into())
    }
}

impl From<NoteCommitment> for [u8; 32] {
    fn from(cm: NoteCommitment) -> [u8; 32] {
        *cm.0
    }
}

impl From<&NoteCommitment> for [u8; 32] {
    fn from(cm: &NoteCommitment) -> [u8; 32] {
        *cm.0
    }
}
