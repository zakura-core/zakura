//! Signature hashes for Zcash transactions

mod v4;

use std::sync::Arc;

use zcash_protocol::value::ZatBalance;
use zcash_transparent::sighash::SighashType;

use super::{zip244::Zip244SighashCache, Transaction};

use crate::parameters::NetworkUpgrade;
use crate::{transparent, Error};

use crate::primitives::zcash_primitives::{sighash, sighash_v4_raw, PrecomputedTxData};
use v4::V4Sighash;

/// The canonical signature hash types.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum HashType {
    /// Sign all the outputs.
    All,
    /// Sign none of the outputs.
    None,
    /// Sign the output with the same index as the transparent input.
    Single,
    /// Sign all outputs and allow additional inputs.
    AllAnyoneCanPay,
    /// Sign no outputs and allow additional inputs.
    NoneAnyoneCanPay,
    /// Sign the corresponding output and allow additional inputs.
    SingleAnyoneCanPay,
}

impl HashType {
    /// Sign all the outputs.
    pub const ALL: Self = Self::All;
    /// Sign none of the outputs.
    pub const NONE: Self = Self::None;
    /// Sign the output with the same index as the transparent input.
    pub const SINGLE: Self = Self::Single;
    /// Sign all outputs and allow additional inputs.
    pub const ALL_ANYONECANPAY: Self = Self::AllAnyoneCanPay;
    /// Sign no outputs and allow additional inputs.
    pub const NONE_ANYONECANPAY: Self = Self::NoneAnyoneCanPay;
    /// Sign the corresponding output and allow additional inputs.
    pub const SINGLE_ANYONECANPAY: Self = Self::SingleAnyoneCanPay;

    /// Parses a canonical signature hash type.
    pub fn from_bits(bits: u32) -> Option<Self> {
        let raw = bits.try_into().ok()?;
        SighashType::parse(raw)?.try_into().ok()
    }

    /// Returns the encoded signature hash type.
    pub fn bits(self) -> u32 {
        u32::from(SighashType::from(self).encode())
    }

    pub(crate) fn anyone_can_pay(self) -> bool {
        matches!(
            self,
            Self::AllAnyoneCanPay | Self::NoneAnyoneCanPay | Self::SingleAnyoneCanPay
        )
    }

    pub(crate) fn is_single(self) -> bool {
        matches!(self, Self::Single | Self::SingleAnyoneCanPay)
    }

    pub(crate) fn is_none(self) -> bool {
        matches!(self, Self::None | Self::NoneAnyoneCanPay)
    }
}

impl From<HashType> for SighashType {
    fn from(hash_type: HashType) -> Self {
        match hash_type {
            HashType::All => Self::ALL,
            HashType::None => Self::NONE,
            HashType::Single => Self::SINGLE,
            HashType::AllAnyoneCanPay => Self::ALL_ANYONECANPAY,
            HashType::NoneAnyoneCanPay => Self::NONE_ANYONECANPAY,
            HashType::SingleAnyoneCanPay => Self::SINGLE_ANYONECANPAY,
        }
    }
}

impl TryFrom<SighashType> for HashType {
    type Error = ();

    fn try_from(hash_type: SighashType) -> Result<Self, Self::Error> {
        Ok(match hash_type {
            SighashType::ALL => Self::All,
            SighashType::NONE => Self::None,
            SighashType::SINGLE => Self::Single,
            SighashType::ALL_ANYONECANPAY => Self::AllAnyoneCanPay,
            SighashType::NONE_ANYONECANPAY => Self::NoneAnyoneCanPay,
            SighashType::SINGLE_ANYONECANPAY => Self::SingleAnyoneCanPay,
            _other => return Err(()),
        })
    }
}

/// A Signature Hash (or SIGHASH) as specified in
/// <https://zips.z.cash/protocol/protocol.pdf#sighash>
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct SigHash(pub [u8; 32]);

impl AsRef<[u8; 32]> for SigHash {
    fn as_ref(&self) -> &[u8; 32] {
        &self.0
    }
}

impl AsRef<[u8]> for SigHash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<SigHash> for [u8; 32] {
    fn from(sighash: SigHash) -> Self {
        sighash.0
    }
}

/// A SigHasher context which stores precomputed data that is reused
/// between sighash computations for the same transaction.
#[derive(Debug)]
pub struct SigHasher {
    precomputed_tx_data: PrecomputedTxData,
    v4: Option<V4Sighash>,
    zip244: Option<Zip244SighashCache>,
}

impl SigHasher {
    /// Create a new SigHasher for the given transaction.
    ///
    /// # Panics
    ///
    /// - If `trans` can't be converted to its `librustzcash` equivalent. This could happen, for
    ///   example, if `trans` contains the `nConsensusBranchId` field, and `nu` doesn't match it.
    ///   More details in [`PrecomputedTxData::new`].
    /// - If `nu` doesn't contain a consensus branch id convertible to its `librustzcash`
    ///   equivalent.
    pub fn new(
        trans: &Transaction,
        nu: NetworkUpgrade,
        all_previous_outputs: Arc<Vec<transparent::Output>>,
    ) -> Result<Self, Error> {
        let precomputed_tx_data = PrecomputedTxData::new(trans, nu, all_previous_outputs.clone())?;

        Ok(SigHasher {
            precomputed_tx_data,
            v4: V4Sighash::new(trans, nu, &all_previous_outputs),
            zip244: Zip244SighashCache::new(trans, &all_previous_outputs),
        })
    }

    /// Calculate the sighash for the current transaction.
    ///
    /// # Details
    ///
    /// The `input_index_script_code` tuple contains the transparent input index
    /// and the script code being validated, or `None` for a shielded signature.
    /// Pre-V5 sighashes commit to the supplied script code. V5 and later
    /// transactions ignore it because ZIP-244 commits to the spent output's
    /// `scriptPubKey` instead.
    ///
    /// This method only calculates a digest. For V5 and later transactions,
    /// callers must separately reject `SIGHASH_SINGLE` when the transparent
    /// input does not have a corresponding output. Callers that need raw
    /// pre-V5 hash type bytes must use [`SigHasher::sighash_v4_raw`].
    ///
    /// # Panics
    ///
    /// - If the input index is out of bounds for the transaction inputs.
    pub fn sighash(
        &self,
        hash_type: HashType,
        input_index_script_code: Option<(usize, Vec<u8>)>,
    ) -> SigHash {
        let canonical_hash_type: SighashType = hash_type.into();

        if let Some(zip244) = &self.zip244 {
            return zip244.sighash(
                hash_type,
                input_index_script_code.as_ref().map(|(index, _)| *index),
            );
        }

        if let Some(v4) = &self.v4 {
            return v4
                .signature_hash(
                    canonical_hash_type.encode(),
                    input_index_script_code
                        .as_ref()
                        .map(|(index, script_code)| (*index, script_code.as_slice())),
                )
                .expect(
                    "sighash precondition violated: callers must pass an in-bounds input_index",
                );
        }

        sighash(
            &self.precomputed_tx_data,
            hash_type,
            input_index_script_code,
        )
    }

    /// Calculate the sighash for the current pre-V5 transaction using the
    /// raw `hash_type` byte taken directly from the signature.
    ///
    /// This preserves non-canonical bits (e.g. `0x41`) in the preimage so that
    /// the resulting digest matches `zcashd`'s pre-V5 sighash semantics.
    /// Callers handling V5+ transactions must use [`SigHasher::sighash`].
    /// Returns `None` for V5+ transactions.
    pub fn sighash_v4_raw(
        &self,
        raw_hash_type: u8,
        input_index_script_code: Option<(usize, Vec<u8>)>,
    ) -> Option<SigHash> {
        if self.zip244.is_some() {
            return None;
        }

        if let Some(v4) = &self.v4 {
            return v4.signature_hash(
                raw_hash_type,
                input_index_script_code
                    .as_ref()
                    .map(|(index, script_code)| (*index, script_code.as_slice())),
            );
        }

        Some(sighash_v4_raw(
            &self.precomputed_tx_data,
            raw_hash_type,
            input_index_script_code,
        ))
    }

    /// Returns the Orchard bundle in the precomputed transaction data.
    pub fn orchard_bundle(
        &self,
    ) -> Option<::orchard::bundle::Bundle<::orchard::bundle::Authorized, ZatBalance>> {
        self.precomputed_tx_data.orchard_bundle()
    }

    /// Returns the Ironwood bundle in the precomputed transaction data.
    pub fn ironwood_bundle(
        &self,
    ) -> Option<::orchard::bundle::Bundle<::orchard::bundle::Authorized, ZatBalance>> {
        self.precomputed_tx_data.ironwood_bundle()
    }

    /// Returns the Sapling bundle in the precomputed transaction data.
    pub fn sapling_bundle(
        &self,
    ) -> Option<sapling_crypto::Bundle<sapling_crypto::bundle::Authorized, ZatBalance>> {
        self.precomputed_tx_data.sapling_bundle()
    }
}

#[cfg(test)]
mod tests {
    use super::{HashType, SighashType};

    #[test]
    fn hash_type_only_accepts_canonical_values() {
        for (raw, expected) in [
            (0x01, HashType::ALL),
            (0x02, HashType::NONE),
            (0x03, HashType::SINGLE),
            (0x81, HashType::ALL_ANYONECANPAY),
            (0x82, HashType::NONE_ANYONECANPAY),
            (0x83, HashType::SINGLE_ANYONECANPAY),
        ] {
            assert_eq!(HashType::from_bits(raw), Some(expected));
            assert_eq!(expected.bits(), raw);
        }

        for raw in [0x00, 0x04, 0x41, 0x80, 0x84, 0x100] {
            assert_eq!(HashType::from_bits(raw), None);
        }

        assert!(HashType::try_from(SighashType::from_raw(0x41)).is_err());
    }
}
