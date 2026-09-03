//! Orchard notes

use group::{ff::PrimeField, GroupEncoding};
use halo2::pasta::pallas;
use rand_core::{CryptoRng, RngCore};

use crate::error::{NoteError, RandError};

use super::sinsemilla::extract_p;

mod ciphertexts;
mod nullifiers;

pub use ciphertexts::{EncryptedNote, WrappedNoteKey};
pub use nullifiers::Nullifier;

#[cfg(any(test, feature = "proptest-impl"))]
mod arbitrary;

/// Used as input to PRF^nf as part of deriving the _nullifier_ of the _note_.
///
/// When creating a new note from spending an old note, the new note's _rho_ is
/// the _nullifier_ of the previous note. If creating a note from scratch (like
/// a miner reward), a dummy note is constructed, and its nullifier as the _rho_
/// for the actual output note. When creating a dummy note, its _rho_ is chosen
/// as a random Pallas point's x-coordinate.
///
/// <https://zips.z.cash/protocol/nu5.pdf#orcharddummynotes>
#[derive(Clone, Debug)]
pub struct Rho(pub(crate) pallas::Base);

impl From<Rho> for [u8; 32] {
    fn from(rho: Rho) -> Self {
        rho.0.to_repr()
    }
}

impl From<Nullifier> for Rho {
    fn from(nf: Nullifier) -> Self {
        Self(nf.0)
    }
}

impl Rho {
    pub fn new<T>(csprng: &mut T) -> Result<Self, NoteError>
    where
        T: RngCore + CryptoRng,
    {
        let mut bytes = [0u8; 32];
        csprng
            .try_fill_bytes(&mut bytes)
            .map_err(|_| NoteError::from(RandError::FillBytes))?;

        let possible_point = pallas::Point::from_bytes(&bytes);

        if possible_point.is_some().into() {
            Ok(Self(extract_p(possible_point.unwrap())))
        } else {
            Err(NoteError::InvalidRho)
        }
    }
}

/// Additional randomness used in deriving the _nullifier_.
///
/// <https://zips.z.cash/protocol/nu5.pdf#orchardsend>
#[derive(Clone, Debug)]
pub struct Psi(pub(crate) pallas::Base);

impl From<Psi> for [u8; 32] {
    fn from(psi: Psi) -> Self {
        psi.0.to_repr()
    }
}
