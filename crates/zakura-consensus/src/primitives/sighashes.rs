//! The digests that the signatures of a shielded bundle sign.

use std::sync::Arc;

use zakura_chain::transaction::{v7::ShieldedSighashes, SigHash};

/// The digests that the signatures of a Sapling, Orchard, or Ironwood bundle sign.
#[derive(Clone, Debug)]
pub enum BundleSighashes {
    /// One digest that every signature of a bundle before v7 signs.
    Shared(SigHash),
    /// One digest per signature, for a bundle of a v7 transaction ([ZIP 248]).
    ///
    /// [ZIP 248]: https://zips.z.cash/zip-0248
    PerSignature(Arc<ShieldedSighashes>),
}

impl BundleSighashes {
    /// Returns a digest that determines every digest the bundle's signatures sign, for
    /// the verification cache key.
    ///
    /// A v7 binding signature's digest commits to the transaction identifier, the spent
    /// outputs, and the bundle, which are everything a spend authorization digest commits
    /// to except its position.
    pub(crate) fn commitment(&self) -> [u8; 32] {
        match self {
            Self::Shared(sighash) => sighash.0,
            Self::PerSignature(sighashes) => sighashes.binding(),
        }
    }
}

impl From<SigHash> for BundleSighashes {
    fn from(sighash: SigHash) -> Self {
        Self::Shared(sighash)
    }
}

impl orchard::bundle::SignatureDigests for BundleSighashes {
    fn spend_auth(&self, index: usize) -> Option<[u8; 32]> {
        match self {
            Self::Shared(sighash) => Some(sighash.0),
            Self::PerSignature(sighashes) => sighashes.spend_auth(index),
        }
    }

    fn binding(&self) -> [u8; 32] {
        self.commitment()
    }
}

impl sapling_crypto::bundle::SignatureDigests for BundleSighashes {
    fn spend_auth(&self, index: usize) -> Option<[u8; 32]> {
        match self {
            Self::Shared(sighash) => Some(sighash.0),
            Self::PerSignature(sighashes) => sighashes.spend_auth(index),
        }
    }

    fn binding(&self) -> [u8; 32] {
        self.commitment()
    }
}
