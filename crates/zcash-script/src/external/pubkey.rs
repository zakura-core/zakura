#[cfg(feature = "signature-validation")]
use secp256k1::{ecdsa, Message, PublicKey, Secp256k1};

#[cfg_attr(not(feature = "signature-validation"), allow(dead_code))]
pub(crate) struct PubKey<'a>(pub(crate) &'a [u8]);

impl PubKey<'_> {
    pub(crate) const SIZE: usize = 65;
    pub(crate) const COMPRESSED_SIZE: usize = 33;

    /// Check syntactic correctness.
    ///
    /// Note that this is consensus-critical as `check_sig` calls it!
    #[cfg(feature = "signature-validation")]
    pub(crate) fn is_valid(&self) -> bool {
        !self.0.is_empty()
    }

    /// Verify a signature (~72 bytes).
    /// If this public key is not fully valid, the return value will be false.
    #[cfg(feature = "signature-validation")]
    pub(crate) fn verify(&self, hash: &[u8; 32], sig: &ecdsa::Signature) -> bool {
        if !self.is_valid() {
            return false;
        };

        if let Ok(pubkey) = PublicKey::from_slice(self.0) {
            let mut normalized_sig = *sig;
            // libsecp256k1's ECDSA verification requires lower-S signatures, which are
            // not required by consensus in Zcash, so normalize them first.
            normalized_sig.normalize_s();
            let secp = Secp256k1::verification_only();
            secp.verify_ecdsa(&Message::from_digest(*hash), &normalized_sig, &pubkey)
                .is_ok()
        } else {
            false
        }
    }

    #[cfg(feature = "signature-validation")]
    pub(crate) fn check_low_s(sig: &ecdsa::Signature) -> bool {
        let mut check = *sig;
        check.normalize_s();
        sig == &check
    }
}
