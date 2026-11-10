//! Sapling-related functionality.
//!
//! These data structures enforce the *structural validity* of Sapling-related
//! consensus-critical objects.
//!
//! **Consensus rule**:
//!
//! These data structures ensure that [ZIP-216](https://zips.z.cash/zip-0216),
//! canonical Jubjub point encodings, are enforced everywhere where Jubjub
//! points occur, and non-canonical point encodings are rejected. This is
//! enforced by the jubjub crate, which is also used by the redjubjub crate.

mod commitment;
mod note;

#[cfg(any(test, feature = "proptest-impl"))]
mod arbitrary;
#[cfg(test)]
mod tests;

pub mod keys;
pub mod output;
pub mod shielded_data;
pub mod spend;
pub mod tree;

pub use commitment::ValueCommitment;
pub use note::{EncryptedNote, Nullifier, WrappedNoteKey};
pub use output::{Output, OutputInTransactionV4, OutputPrefixInTransactionV5};
pub use shielded_data::{
    AnchorVariant, FieldNotPresent, PerSpendAnchor, SharedAnchor, ShieldedData, TransferData,
};
pub use spend::{Spend, SpendPrefixInTransactionV5};

/// Returns true if every encoding is a canonical, non-small-order Jubjub point:
/// the check [`ValueCommitment::is_valid_not_small_order`],
/// [`keys::EphemeralPublicKey::is_valid_not_small_order`] and
/// [`keys::ValidatingKey::is_valid_not_small_order`] run on one encoding.
///
/// The points are decompressed together, so their field inversions are batched
/// into one. Each square root is still computed separately.
pub(crate) fn all_valid_not_small_order(encodings: impl Iterator<Item = [u8; 32]>) -> bool {
    jubjub::AffinePoint::batch_from_bytes(encodings)
        .into_iter()
        .all(|point| {
            point
                .into_option()
                .is_some_and(|point| !bool::from(point.is_small_order()))
        })
}
