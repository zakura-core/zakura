//! ML-KEM-1024 (FIPS 203), the post-quantum half of the hybrid key exchange
//! (TLS-9).
//!
//! The arithmetic is `libcrux-ml-kem`. This module adds what a handshake
//! needs around it: the length checks, the check of a peer's encapsulation
//! key, the random draws and the wiping of the secrets it holds.
//!
//! In one exchange the dialer [generates](MlKem1024::generate) a key pair and
//! sends the encapsulation key. The listener
//! [encapsulates](MlKem1024::encapsulate) to it and returns a ciphertext. The
//! dialer [decapsulates](DecapsulationKey::decapsulate) the ciphertext. Both
//! now hold the same 32-byte secret.
//!
//! Wiping is best effort. The buffers the randomness is drawn into, the
//! private key and the returned secrets are wiped when they drop. libcrux
//! takes the randomness by value and returns keys and secrets by value, so
//! the copies those calls and later moves leave on the stack are not.

use libcrux_ml_kem::{
    mlkem1024::{self, MlKem1024Ciphertext, MlKem1024PrivateKey, MlKem1024PublicKey},
    ENCAPS_SEED_SIZE, KEY_GENERATION_SEED_SIZE, SHARED_SECRET_SIZE,
};
use rustls::{crypto::SecureRandom, PeerMisbehaved};
use zeroize::{Zeroize as _, Zeroizing};

/// The length of an encapsulation key, the share the dialer sends.
pub(crate) const ENCAPSULATION_KEY_LEN: usize = 1568;

/// The length of a ciphertext, the share the listener returns.
pub(crate) const CIPHERTEXT_LEN: usize = 1568;

/// The secret both sides derive. It's wiped when dropped.
pub(crate) type SharedSecret = Zeroizing<[u8; SHARED_SECRET_SIZE]>;

/// ML-KEM-1024 with the random source its key pairs and encapsulations draw
/// from.
///
/// libcrux takes its randomness as an argument, so the source decides every
/// byte: a simulation that seeds the source replays the same shares.
#[derive(Debug)]
pub(crate) struct MlKem1024 {
    random: &'static dyn SecureRandom,
}

impl MlKem1024 {
    /// ML-KEM-1024 drawing from `random`.
    pub(crate) const fn new(random: &'static dyn SecureRandom) -> Self {
        Self { random }
    }

    /// Generates the dialer's key pair from one 64-byte draw.
    ///
    /// # Errors
    ///
    /// Fails if the random source does.
    pub(crate) fn generate(&self) -> Result<DecapsulationKey, rustls::Error> {
        let mut seed = Zeroizing::new([0; KEY_GENERATION_SEED_SIZE]);
        self.random.fill(seed.as_mut_slice())?;
        let (private, public) = mlkem1024::generate_key_pair(*seed).into_parts();
        Ok(DecapsulationKey { private, public })
    }

    /// Encapsulates to the dialer's `encapsulation_key` with one 32-byte
    /// draw. Returns the ciphertext to send back and the shared secret.
    ///
    /// # Errors
    ///
    /// Refuses a key of the wrong length, and one that isn't the canonical
    /// encoding of its own coefficients (the FIPS 203 §7.2 modulus check),
    /// before drawing anything. libcrux's `encapsulate` doesn't run that
    /// check itself. Also fails if the random source does.
    pub(crate) fn encapsulate(
        &self,
        encapsulation_key: &[u8],
    ) -> Result<([u8; CIPHERTEXT_LEN], SharedSecret), rustls::Error> {
        let encapsulation_key = MlKem1024PublicKey::try_from(encapsulation_key)
            .map_err(|_| PeerMisbehaved::InvalidKeyShare)?;
        if !mlkem1024::validate_public_key(&encapsulation_key) {
            return Err(PeerMisbehaved::InvalidKeyShare.into());
        }
        let mut randomness = Zeroizing::new([0; ENCAPS_SEED_SIZE]);
        self.random.fill(randomness.as_mut_slice())?;
        let (ciphertext, secret) = mlkem1024::encapsulate(&encapsulation_key, *randomness);
        Ok((*ciphertext.as_slice(), Zeroizing::new(secret)))
    }
}

/// The dialer's key pair for one exchange.
///
/// The private half is wiped when this drops, as far as the module docs
/// say wiping reaches.
pub(crate) struct DecapsulationKey {
    private: MlKem1024PrivateKey,
    public: MlKem1024PublicKey,
}

impl DecapsulationKey {
    /// The encapsulation key, the share to send to the listener.
    pub(crate) fn encapsulation_key(&self) -> &[u8; ENCAPSULATION_KEY_LEN] {
        self.public.as_slice()
    }

    /// The secret the listener encapsulated in `ciphertext`.
    ///
    /// A ciphertext of the right length always gives a secret. One that
    /// wasn't made for this key gives an unrelated secret (FIPS 203's
    /// implicit rejection), so the handshake fails when the two sides' keys
    /// don't match, without telling the sender why.
    ///
    /// # Errors
    ///
    /// Refuses a ciphertext of the wrong length.
    pub(crate) fn decapsulate(&self, ciphertext: &[u8]) -> Result<SharedSecret, rustls::Error> {
        let ciphertext = MlKem1024Ciphertext::try_from(ciphertext)
            .map_err(|_| PeerMisbehaved::InvalidKeyShare)?;
        Ok(Zeroizing::new(mlkem1024::decapsulate(
            &self.private,
            &ciphertext,
        )))
    }
}

impl Drop for DecapsulationKey {
    /// Wipes the private key.
    fn drop(&mut self) {
        self.private[0..].zeroize();
    }
}

#[cfg(test)]
mod tests;
