//! Equihash Solution and related items.

use std::{fmt, io};

use hex::{FromHex, FromHexError, ToHex};
use serde_big_array::BigArray;

use crate::{
    block::Header,
    parameters::Network,
    serialization::{
        zcash_deserialize_bytes_external_count, zcash_serialize_bytes, CompactSizeMessage,
        SerializationError, ZcashDeserialize, ZcashDeserializeInto, ZcashSerialize,
    },
};

#[cfg(feature = "internal-miner")]
mod internal_miner;

#[cfg(feature = "internal-miner")]
pub use internal_miner::{SolverAction, SolverCancelled};

/// The error type for Equihash validation.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The solution failed Equihash verification under the parameters
    /// required by the active network.
    #[error("invalid equihash solution for BlockHeader")]
    Equihash(#[from] equihash::Error),

    /// The solution's size does not match the Equihash parameters required by
    /// the active network (for example, a 36-byte Regtest-shaped solution
    /// submitted on Mainnet or Testnet).
    #[error("equihash solution size is invalid for the {network} network")]
    InvalidSolutionSize {
        /// The network whose Equihash parameters the solution violated.
        network: Network,
    },
}

/// The size of an Equihash solution in bytes (always 1344).
pub(crate) const SOLUTION_SIZE: usize = 1344;

/// The size of an Equihash solution in bytes on Regtest (always 36).
pub(crate) const REGTEST_SOLUTION_SIZE: usize = 36;

/// The Equihash `N` parameter on Regtest, matching zcashd's regtest chain parameters.
const REGTEST_N: u32 = 48;

/// The Equihash `K` parameter on Regtest, matching zcashd's regtest chain parameters.
const REGTEST_K: u32 = 5;

/// Equihash Solution in compressed format.
///
/// A wrapper around `[u8; n]` where `n` is the solution size because
/// Rust doesn't implement common traits like `Debug`, `Clone`, etc.
/// for collections like arrays beyond lengths 0 to 32.
///
/// The size of an Equihash solution in bytes is always 1344 on Mainnet and Testnet, and
/// is always 36 on Regtest so the length of this type is fixed.
#[derive(Deserialize, Serialize)]
// It's okay to use the extra space on Regtest
#[allow(clippy::large_enum_variant)]
pub enum Solution {
    /// Equihash solution on Mainnet or Testnet
    Common(#[serde(with = "BigArray")] [u8; SOLUTION_SIZE]),
    /// Equihash solution on Regtest
    Regtest(#[serde(with = "BigArray")] [u8; REGTEST_SOLUTION_SIZE]),
}

impl Solution {
    /// The length of the portion of the header used as input when verifying
    /// equihash solutions, in bytes.
    ///
    /// Excludes the 32-byte nonce, which is passed as a separate argument
    /// to the verification function.
    pub const INPUT_LENGTH: usize = 4 + 32 * 3 + 4 * 2;

    /// Returns the inner value of the [`Solution`] as a byte slice.
    fn value(&self) -> &[u8] {
        match self {
            Solution::Common(solution) => solution.as_slice(),
            Solution::Regtest(solution) => solution.as_slice(),
        }
    }

    /// Returns `Ok(())` if this solution is a valid Equihash solution for
    /// `header` under the Equihash parameters required by `network`.
    ///
    /// The `(n, k)` Equihash parameters are chosen from `network`, never from
    /// the attacker-controlled solution length. A solution whose variant does
    /// not match the parameters `network` requires is rejected: Mainnet and
    /// Testnet require the memory-hard `(200, 9)` [`Solution::Common`]
    /// solution, while Regtest requires the toy `(48, 5)`
    /// [`Solution::Regtest`] solution. This prevents a peer from changing the
    /// proof-of-work parameters by choosing the on-wire solution length.
    #[allow(clippy::unwrap_in_result)]
    pub fn check(&self, header: &Header, network: &Network) -> Result<(), Error> {
        self.validate_shape(network)?;

        // TODO:
        // - Add Equihash parameters field to `testnet::Parameters`
        // - Update `Solution::Regtest` variant to hold a `Vec` to support arbitrary parameters - rename to `Other`
        let (n, k) = if network.is_regtest() {
            (REGTEST_N, REGTEST_K)
        } else {
            (200, 9)
        };

        self.check_equihash(header, n, k)
    }

    /// Validate only the solution's encoded shape against the authenticated network parameters.
    pub fn validate_shape(&self, network: &Network) -> Result<(), Error> {
        match (network.is_regtest(), self) {
            // Mainnet and Testnet require the memory-hard (200, 9) parameters,
            // encoded as a 1344-byte `Common` solution.
            (false, Solution::Common(_)) => Ok(()),
            // Regtest requires the toy (48, 5) parameters used by zcashd.
            (true, Solution::Regtest(_)) => Ok(()),
            // Reject a solution variant that does not match the active
            // network before selecting parameters. Otherwise the
            // attacker-controlled solution length could change the PoW
            // parameter set.
            (false, Solution::Regtest(_)) | (true, Solution::Common(_)) => {
                Err(Error::InvalidSolutionSize {
                    network: network.clone(),
                })
            }
        }
    }

    /// Returns `Ok(())` if this solution is valid for `header` under the given
    /// Equihash `(n, k)` parameters.
    ///
    /// The parameters are supplied explicitly and must be chosen from a trusted
    /// source, never derived from the solution length. Callers on the block
    /// ingestion path must go through [`Solution::check`], which binds `(n, k)`
    /// to the active network.
    #[allow(clippy::unwrap_in_result)]
    fn check_equihash(&self, header: &Header, n: u32, k: u32) -> Result<(), Error> {
        let nonce = &header.nonce;

        let mut input = Vec::new();
        header
            .zcash_serialize(&mut input)
            .expect("serialization into a vec can't fail");

        // The part of the header before the nonce and solution.
        // This data is kept constant during solver runs, so the verifier API takes it separately.
        let input = &input[0..Solution::INPUT_LENGTH];

        equihash::is_valid_solution(n, k, input, nonce.as_ref(), self.value())?;

        Ok(())
    }

    /// Returns a [`Solution`] containing the bytes from `solution`.
    /// Returns an error if `solution` is the wrong length.
    pub fn from_bytes(solution: &[u8]) -> Result<Self, SerializationError> {
        match solution.len() {
            // Won't panic, because we just checked the length.
            SOLUTION_SIZE => {
                let mut bytes = [0; SOLUTION_SIZE];
                bytes.copy_from_slice(solution);
                Ok(Self::Common(bytes))
            }
            REGTEST_SOLUTION_SIZE => {
                let mut bytes = [0; REGTEST_SOLUTION_SIZE];
                bytes.copy_from_slice(solution);
                Ok(Self::Regtest(bytes))
            }
            _unexpected_len => Err(SerializationError::Parse(
                "incorrect equihash solution size",
            )),
        }
    }

    /// Returns a [`Solution`] of `[0; SOLUTION_SIZE]` to be used in block proposals.
    pub fn for_proposal() -> Self {
        Self::Common([0; SOLUTION_SIZE])
    }

    /// Returns a null [`Solution`] to be used in block proposals on `network`.
    pub fn for_proposal_for_network(network: &Network) -> Self {
        if network.is_regtest() {
            Self::Regtest([0; REGTEST_SOLUTION_SIZE])
        } else {
            Self::for_proposal()
        }
    }
}

impl PartialEq<Solution> for Solution {
    fn eq(&self, other: &Solution) -> bool {
        self.value() == other.value()
    }
}

impl fmt::Debug for Solution {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("EquihashSolution")
            .field(&hex::encode(self.value()))
            .finish()
    }
}

// These impls all only exist because of array length restrictions.

impl Copy for Solution {}

impl Clone for Solution {
    fn clone(&self) -> Self {
        *self
    }
}

impl Eq for Solution {}

#[cfg(any(test, feature = "proptest-impl"))]
impl Default for Solution {
    fn default() -> Self {
        Self::Common([0; SOLUTION_SIZE])
    }
}

impl ZcashSerialize for Solution {
    fn zcash_serialize<W: io::Write>(&self, writer: W) -> Result<(), io::Error> {
        zcash_serialize_bytes(&self.value().to_vec(), writer)
    }
}

impl ZcashDeserialize for Solution {
    fn zcash_deserialize<R: io::Read>(mut reader: R) -> Result<Self, SerializationError> {
        let len: CompactSizeMessage = (&mut reader).zcash_deserialize_into()?;
        let len: usize = len.into();

        // Validate the length against the consensus-required sizes before
        // allocating, so an attacker-controlled CompactSize cannot force a
        // multi-megabyte allocation.
        if len > SOLUTION_SIZE {
            return Err(SerializationError::Parse(
                "incorrect equihash solution size",
            ));
        }

        let solution = zcash_deserialize_bytes_external_count(len, &mut reader)?;
        Self::from_bytes(&solution)
    }
}

impl ToHex for &Solution {
    fn encode_hex<T: FromIterator<char>>(&self) -> T {
        self.value().encode_hex()
    }

    fn encode_hex_upper<T: FromIterator<char>>(&self) -> T {
        self.value().encode_hex_upper()
    }
}

impl ToHex for Solution {
    fn encode_hex<T: FromIterator<char>>(&self) -> T {
        (&self).encode_hex()
    }

    fn encode_hex_upper<T: FromIterator<char>>(&self) -> T {
        (&self).encode_hex_upper()
    }
}

impl FromHex for Solution {
    type Error = FromHexError;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        let bytes = Vec::from_hex(hex)?;
        Solution::from_bytes(&bytes).map_err(|_| FromHexError::InvalidStringLength)
    }
}
