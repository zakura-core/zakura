//! Sprout nullifiers.

use serde::{Deserialize, Serialize};

use crate::fmt::HexDebug;

/// A Nullifier for Sprout transactions
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(
    any(test, feature = "proptest-impl"),
    derive(proptest_derive::Arbitrary)
)]
pub struct Nullifier(pub HexDebug<[u8; 32]>);

impl Nullifier {
    /// Return the bytes in big-endian byte order as required
    /// by RPCs such as `getrawtransaction`.
    pub fn bytes_in_display_order(&self) -> [u8; 32] {
        let mut root: [u8; 32] = self.into();
        root.reverse();
        root
    }
}

impl From<[u8; 32]> for Nullifier {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes.into())
    }
}

impl From<Nullifier> for [u8; 32] {
    fn from(n: Nullifier) -> Self {
        *n.0
    }
}

impl From<&Nullifier> for [u8; 32] {
    fn from(n: &Nullifier) -> Self {
        *n.0
    }
}
