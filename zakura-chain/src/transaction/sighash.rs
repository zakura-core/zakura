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

bitflags::bitflags! {
    /// The different SigHash types, as defined in <https://zips.z.cash/zip-0143>
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct HashType: u32 {
        /// Sign all the outputs
        const ALL = 0b0000_0001;
        /// Sign none of the outputs - anyone can spend
        const NONE = 0b0000_0010;
        /// Sign one of the outputs - anyone can spend the rest
        const SINGLE = Self::ALL.bits() | Self::NONE.bits();
        /// Anyone can add inputs to this transaction
        const ANYONECANPAY = 0b1000_0000;

        /// Sign all the outputs and Anyone can add inputs to this transaction
        const ALL_ANYONECANPAY = Self::ALL.bits() | Self::ANYONECANPAY.bits();
        /// Sign none of the outputs and Anyone can add inputs to this transaction
        const NONE_ANYONECANPAY = Self::NONE.bits() | Self::ANYONECANPAY.bits();
        /// Sign one of the outputs and Anyone can add inputs to this transaction
        const SINGLE_ANYONECANPAY = Self::SINGLE.bits() | Self::ANYONECANPAY.bits();
    }
}

impl TryFrom<HashType> for SighashType {
    type Error = ();

    fn try_from(hash_type: HashType) -> Result<Self, Self::Error> {
        Ok(match hash_type {
            HashType::ALL => Self::ALL,
            HashType::NONE => Self::NONE,
            HashType::SINGLE => Self::SINGLE,
            HashType::ALL_ANYONECANPAY => Self::ALL_ANYONECANPAY,
            HashType::NONE_ANYONECANPAY => Self::NONE_ANYONECANPAY,
            HashType::SINGLE_ANYONECANPAY => Self::SINGLE_ANYONECANPAY,
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
    /// - If `hash_type` is not `ALL`, `NONE`, `SINGLE`, or one of their
    ///   `ANYONECANPAY` variants.
    pub fn sighash(
        &self,
        hash_type: HashType,
        input_index_script_code: Option<(usize, Vec<u8>)>,
    ) -> SigHash {
        let canonical_hash_type: SighashType =
            hash_type.try_into().expect("hash type should be canonical");

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
    pub fn sighash_v4_raw(
        &self,
        raw_hash_type: u8,
        input_index_script_code: Option<(usize, Vec<u8>)>,
    ) -> SigHash {
        if let Some(v4) = &self.v4 {
            return v4
                .signature_hash(
                    raw_hash_type,
                    input_index_script_code
                        .as_ref()
                        .map(|(index, script_code)| (*index, script_code.as_slice())),
                )
                .expect(
                    "sighash precondition violated: callers must pass an in-bounds input_index",
                );
        }

        sighash_v4_raw(
            &self.precomputed_tx_data,
            raw_hash_type,
            input_index_script_code,
        )
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
