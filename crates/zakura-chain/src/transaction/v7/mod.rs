//! The v7 extensible transaction format of [ZIP 248].
//!
//! A [`Transaction::V7`] represents the bundles ZIP 248 registers. Its Sapling, Orchard,
//! and Ironwood bundles carry their balancing values as their value balances; its
//! [`V7Fields`] carry the other value pool deltas and the coinbase bundle.
//!
//! The private children hold the encoding (`codec`), the transaction identifier and
//! authorizing data commitment (`digest`), and the signature digests (`sighash`).
//!
//! [ZIP 248]: https://zips.z.cash/zip-0248

mod codec;
mod deltas;
mod digest;
mod fields;
mod registry;
mod sighash;

#[cfg(test)]
mod tests;

pub use fields::{
    CoinbaseBundle, CoinbaseError, MinerPoll, MinerSignal, V7Fields, ValueDeltas,
    MAX_COINBASE_DATA_LEN,
};
pub use sighash::{ShieldedBundle, ShieldedSighashes};

pub(crate) use codec::{read, V7_VERSION_GROUP_ID};
pub(crate) use sighash::V7SighashCache;

use crate::{
    block, orchard,
    parameters::NetworkUpgrade,
    sapling,
    transaction::{AuthDigest, Hash, LockTime, Transaction},
    transparent,
};

/// A borrowed view of a [`Transaction::V7`], for encoding and hashing it.
pub(crate) struct V7Ref<'a> {
    network_upgrade: NetworkUpgrade,
    lock_time: LockTime,
    expiry_height: block::Height,
    inputs: &'a [transparent::Input],
    outputs: &'a [transparent::Output],
    sapling: Option<&'a sapling::ShieldedData<sapling::SharedAnchor>>,
    orchard: Option<&'a orchard::ShieldedData>,
    ironwood: Option<&'a orchard::ShieldedData>,
    fields: &'a V7Fields,
}

impl<'a> V7Ref<'a> {
    /// Borrows the parts of a v7 transaction, or returns `None` for other versions.
    pub(crate) fn of(tx: &'a Transaction) -> Option<Self> {
        let Transaction::V7 {
            network_upgrade,
            lock_time,
            expiry_height,
            inputs,
            outputs,
            sapling_shielded_data,
            orchard_shielded_data,
            ironwood_shielded_data,
            v7_fields,
        } = tx
        else {
            return None;
        };
        Some(Self {
            network_upgrade: *network_upgrade,
            lock_time: *lock_time,
            expiry_height: *expiry_height,
            inputs,
            outputs,
            sapling: sapling_shielded_data.as_deref(),
            orchard: orchard_shielded_data.as_deref(),
            ironwood: ironwood_shielded_data.as_deref(),
            fields: v7_fields,
        })
    }

    /// Writes the transaction after its `header` field.
    pub(crate) fn write<W: std::io::Write + ?Sized>(&self, writer: &mut W) -> std::io::Result<()> {
        codec::write(self, writer)
    }

    /// Returns the transaction identifier.
    pub(crate) fn txid(&self) -> Hash {
        Hash(self.txid_digest())
    }

    /// Returns `true` if the transaction has a transparent effecting bundle: a v7
    /// transparent bundle with neither inputs nor outputs is encoded by omitting it.
    fn has_transparent_effects(&self) -> bool {
        !(self.inputs.is_empty() && self.outputs.is_empty())
    }

    /// Returns the authorizing data commitment.
    pub(crate) fn authorizing_digest(&self) -> AuthDigest {
        AuthDigest(self.auth_digest())
    }
}
