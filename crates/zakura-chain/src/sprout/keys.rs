//! Sprout key types.
//!
//! Unused key types are not implemented, see PR #5476.
//!
//! "The receiving key sk_enc, the incoming viewing key ivk = (apk,
//! sk_enc), and the shielded payment address addr_pk = (a_pk, pk_enc) are
//! derived from a_sk, as described in ['Sprout Key Components'][ps]
//!
//! [ps]: https://zips.z.cash/protocol/protocol.pdf#sproutkeycomponents

use std::fmt;

/// A Sprout _paying key_.
///
/// Derived from a Sprout _spending key.
#[derive(Copy, Clone, Eq, PartialEq)]
#[cfg_attr(
    any(test, feature = "proptest-impl"),
    derive(proptest_derive::Arbitrary)
)]
pub struct PayingKey(pub [u8; 32]);

impl AsRef<[u8]> for PayingKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for PayingKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("PayingKey")
            .field(&hex::encode(self.0))
            .finish()
    }
}

/// A Sprout ephemeral public key, `KA^{Sprout}.Public`.
///
/// Zakura never performs Sprout key agreement, so this is stored and compared
/// as raw bytes.
///
/// <https://zips.z.cash/protocol/protocol.pdf#concretesproutkeyagreement>
#[derive(Copy, Clone, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct EphemeralPublicKey(pub [u8; 32]);

impl EphemeralPublicKey {
    /// Returns a reference to the encoded key bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns the encoded key bytes.
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0
    }
}

impl From<[u8; 32]> for EphemeralPublicKey {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl From<EphemeralPublicKey> for [u8; 32] {
    fn from(key: EphemeralPublicKey) -> Self {
        key.0
    }
}

impl fmt::Debug for EphemeralPublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("EphemeralPublicKey")
            .field(&hex::encode(self.0))
            .finish()
    }
}
