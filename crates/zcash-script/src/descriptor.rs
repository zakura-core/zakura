//! Output Script Descriptors for Zcash.
//!
//! This module provides the subset of [BIP 380] Output Script Descriptors that are valid
//! to use within the Zcash ecosystem.
//!
//! [BIP 380]: https://github.com/bitcoin/bips/blob/master/bip-0380.mediawiki

use alloc::vec::Vec;
use core::fmt;
use core::iter;

use bip32::{ExtendedPublicKey, Prefix};
use ripemd::Ripemd160;
use secp256k1::PublicKey;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    op, pattern,
    script::{self, Evaluable},
    Opcode,
};

mod encoding;
pub use encoding::ParseError;

/// A key expression.
///
/// Specified in [BIP 380]. We only implement the subset that is relevant to [ZIP 48].
///
/// [BIP 380]: https://github.com/bitcoin/bips/blob/master/bip-0380.mediawiki#key-expressions
/// [ZIP 48]: https://zips.z.cash/zip-0048
#[derive(Clone, Debug)]
pub struct KeyExpression {
    origin: Option<KeyOrigin>,
    key: Key,
}

impl KeyExpression {
    /// Produces a key expression for a BIP 32 extended pubkey and a derivation sequence.
    ///
    /// This key expression will evaluate to a single public key.
    ///
    /// Returns `None` if `prefix` is not for a public key, or `derivation` cannot be
    /// derived from `key`.
    pub fn from_xpub(
        origin: Option<KeyOrigin>,
        prefix: Prefix,
        key: ExtendedPublicKey<PublicKey>,
        child: Vec<bip32::ChildNumber>,
    ) -> Option<Self> {
        if prefix.is_public() {
            Some(Self {
                origin,
                key: Key::checked_xpub(prefix, key, child)?,
            })
        } else {
            None
        }
    }

    /// Splits this key expression into its constituent parts.
    pub fn into_parts(self) -> (Option<KeyOrigin>, Key) {
        (self.origin, self.key)
    }
}

/// Key origin information for a [`KeyExpression`].
///
/// Specified in [BIP 380].
///
/// [BIP 380]: https://github.com/bitcoin/bips/blob/master/bip-0380.mediawiki#key-expressions
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyOrigin {
    fingerprint: [u8; 4],
    derivation: Vec<bip32::ChildNumber>,
}

impl KeyOrigin {
    /// Constructs a key origin from its parts.
    pub fn from_parts(fingerprint: [u8; 4], derivation: Vec<bip32::ChildNumber>) -> Self {
        Self {
            fingerprint,
            derivation,
        }
    }

    /// Returns the BIP 32 key fingerprint.
    pub fn fingerprint(&self) -> &[u8; 4] {
        &self.fingerprint
    }

    /// Returns the BIP 32 derivation path.
    pub fn derivation(&self) -> &[bip32::ChildNumber] {
        &self.derivation
    }
}

/// A key that can be used within a [`KeyExpression`].
///
/// Specified in [BIP 380]. We only implement the subset that is relevant to [ZIP 48], and
/// currently only support keys that evaluate to a single public key.
///
/// [BIP 380]: https://github.com/bitcoin/bips/blob/master/bip-0380.mediawiki#key-expressions
/// [ZIP 48]: https://zips.z.cash/zip-0048
#[derive(Clone)]
pub enum Key {
    /// A standalone public key.
    Public {
        /// The key.
        key: PublicKey,
        /// Whether or not the key should be compressed when encoded as hex.
        compressed: bool,
    },
    /// An extended public key.
    Xpub {
        /// The prefix to use when encoding the extended public key.
        prefix: Prefix,
        /// The extended.
        key: ExtendedPublicKey<PublicKey>,
        /// Any BIP 32 derivation steps to be taken after the given extended key.
        child: Vec<bip32::ChildNumber>,
    },
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Public { key, compressed } => f
                .debug_struct("Public")
                .field("key", key)
                .field("compressed", compressed)
                .finish(),
            Self::Xpub { prefix, key, child } => f
                .debug_struct("Xpub")
                .field("prefix", prefix)
                .field("key", key)
                .field("child", child)
                .finish(),
        }
    }
}

impl Key {
    fn checked_xpub(
        prefix: Prefix,
        key: ExtendedPublicKey<PublicKey>,
        child: Vec<bip32::ChildNumber>,
    ) -> Option<Self> {
        let key = Key::Xpub { prefix, key, child };
        (prefix.is_public() && key.derive_leaf().is_ok()).then_some(key)
    }

    /// This is called at construction to ensure the key is valid.
    fn derive_leaf(&self) -> Result<Self, Error> {
        match self {
            Key::Public { .. } => Ok(self.clone()),
            Key::Xpub { prefix, key, child } => {
                let mut curr_key = key.clone();
                for child_number in child {
                    curr_key = curr_key
                        .derive_child(*child_number)
                        .map_err(|_| Error::InvalidKeyExpression)?;
                }
                Ok(Self::Xpub {
                    prefix: *prefix,
                    key: curr_key,
                    child: vec![],
                })
            }
        }
    }

    /// Derives the leaf pubkey corresponding to this key.
    fn derive_and_serialize(&self) -> Vec<u8> {
        match self.derive_leaf().expect("checked at construction") {
            Key::Public { key, compressed } => {
                if compressed {
                    key.serialize().to_vec()
                } else {
                    key.serialize_uncompressed().to_vec()
                }
            }
            Key::Xpub { key, .. } => key.public_key().serialize().to_vec(),
        }
    }
}

/// Produces a P2SH output script.
///
/// Specified in [BIP 381].
///
/// [BIP 381]: https://github.com/bitcoin/bips/blob/master/bip-0381.mediawiki#sh
pub fn sh(script: &script::Redeem) -> script::PubKey {
    let script_hash = Ripemd160::digest(Sha256::digest(script.to_bytes()));
    script::Component(vec![
        op::HASH160,
        Opcode::from(pattern::push_160b_hash(&script_hash.into())),
        op::EQUAL,
    ])
}

/// Produces a threshold multisig output script.
///
/// Use [`sortedmulti`] instead if you are constructing a new P2SH address. This
/// descriptor is provided for cases where you have an existing P2SH address and you
/// cannot use [`sortedmulti`].
///
/// Specified in [BIP 383].
///
/// [BIP 383]: https://github.com/bitcoin/bips/blob/master/bip-0383.mediawiki#sortedmulti
pub fn multi(k: u8, keys: &[KeyExpression]) -> Result<script::Redeem, Error> {
    multi_inner(k, keys, false)
}

/// Produces a threshold multisig output script.
///
/// Specified in [BIP 383].
///
/// [BIP 383]: https://github.com/bitcoin/bips/blob/master/bip-0383.mediawiki#sortedmulti
pub fn sortedmulti(k: u8, keys: &[KeyExpression]) -> Result<script::Redeem, Error> {
    multi_inner(k, keys, true)
}

fn multi_inner(k: u8, keys: &[KeyExpression], sorted: bool) -> Result<script::Redeem, Error> {
    match u8::try_from(keys.len()) {
        Ok(n @ 1..=20) => {
            if k > n {
                Err(Error::InvalidThreshold(k, n))
            } else {
                let k = Opcode::from(pattern::push_num(k.into()));
                let n = Opcode::from(pattern::push_num(n.into()));

                let mut keys = keys
                    .iter()
                    .map(|expr| expr.key.derive_and_serialize())
                    .collect::<Vec<_>>();

                if sorted {
                    // Sort lexicographically.
                    keys.sort();
                }

                Ok(script::Component(
                    iter::empty()
                        .chain(Some(k))
                        .chain(
                            keys.into_iter()
                                .map(|key| op::push_value(&key).expect("short enough")),
                        )
                        .chain([n, op::CHECKMULTISIG])
                        .collect(),
                ))
            }
        }
        _ => Err(Error::TooManyPubKeys(keys.len())),
    }
}

/// Errors that can happen when creating Output Script Descriptors.
#[allow(missing_docs)]
#[derive(Copy, Clone, PartialEq, Eq, Debug, Error)]
pub enum Error {
    #[error("an invalid key expression was provided")]
    InvalidKeyExpression,
    #[error("multi or sortedmulti was called with no keys")]
    NoPubKeys,
    #[error("multi and sortedmulti only support at most 20 keys, but you provided {0}")]
    TooManyPubKeys(usize),
    #[error(
        "multi or sortedmulti was called with a threshold {0} larger than the number of keys ({1})"
    )]
    InvalidThreshold(u8, u8),
}

#[cfg(test)]
mod tests {
    use std::string::ToString;

    use bip32::{ChildNumber, Prefix};

    use super::{multi, sh, sortedmulti, KeyExpression};
    use crate::script;

    /// From https://github.com/bitcoin/bips/blob/master/bip-0383.mediawiki#test-vectors
    #[test]
    fn bip_383_pubkey_test_vectors() {
        const TV_PUBKEY_1: &str =
            "03a34b99f22c790c4e36b2b3c2c35a36db06226e41c692fc82b8b56ac1c540c5bd";
        const TV_PUBKEY_2:&str="04a34b99f22c790c4e36b2b3c2c35a36db06226e41c692fc82b8b56ac1c540c5bd5b8dec5235a0fa8722476c7709c02559e3aa73aa03918ba2d492eea75abea235";
        const TV_REDEEM_SCRIPT: &str = "512103a34b99f22c790c4e36b2b3c2c35a36db06226e41c692fc82b8b56ac1c540c5bd4104a34b99f22c790c4e36b2b3c2c35a36db06226e41c692fc82b8b56ac1c540c5bd5b8dec5235a0fa8722476c7709c02559e3aa73aa03918ba2d492eea75abea23552ae";

        let tv_pubkey_1 = KeyExpression {
            origin: None,
            key: TV_PUBKEY_1.parse().unwrap(),
        };
        let tv_pubkey_2 = KeyExpression {
            origin: None,
            key: TV_PUBKEY_2.parse().unwrap(),
        };
        assert_eq!(tv_pubkey_1.to_string(), TV_PUBKEY_1);
        assert_eq!(tv_pubkey_2.to_string(), TV_PUBKEY_2);

        let tv_redeem_script =
            script::Redeem::parse(&script::Code(hex::decode(TV_REDEEM_SCRIPT).unwrap())).unwrap();

        assert_eq!(
            multi(1, &[tv_pubkey_1.clone(), tv_pubkey_2.clone()]).as_ref(),
            Ok(&tv_redeem_script)
        );

        assert_eq!(
            sortedmulti(1, &[tv_pubkey_2, tv_pubkey_1]),
            Ok(tv_redeem_script),
        );
    }

    /// From https://github.com/bitcoin/bips/blob/master/bip-0383.mediawiki#test-vectors
    #[test]
    fn bip_383_xpub_test_vectors() {
        const TV_XPRV_1: &str = "xprvA1RpRA33e1JQ7ifknakTFpgNXPmW2YvmhqLQYMmrj4xJXXWYpDPS3xz7iAxn8L39njGVyuoseXzU6rcxFLJ8HFsTjSyQbLYnMpCqE2VbFWc";
        const TV_XPRV_2: &str = "xprv9uPDJpEQgRQfDcW7BkF7eTya6RPxXeJCqCJGHuCJ4GiRVLzkTXBAJMu2qaMWPrS7AANYqdq6vcBcBUdJCVVFceUvJFjaPdGZ2y9WACViL4L";
        const TV_SCRIPT_PUBKEY: &str = "a91445a9a622a8b0a1269944be477640eedc447bbd8487";

        let tv_xprv_1 = KeyExpression::from_xpub(
            Some("[00000000/111'/222]".parse().unwrap()),
            Prefix::XPUB,
            // `secp256k1` can parse `xprv` into an `xpub`, internally
            // converting.
            TV_XPRV_1.parse().unwrap(),
            vec![],
        )
        .unwrap();
        let tv_xprv_2 = KeyExpression::from_xpub(
            None,
            Prefix::XPUB,
            TV_XPRV_2.parse().unwrap(),
            vec![ChildNumber::new(0, false).unwrap()],
        )
        .unwrap();

        let tv_script_pubkey =
            script::PubKey::parse(&script::Code(hex::decode(TV_SCRIPT_PUBKEY).unwrap())).unwrap();

        assert_eq!(
            multi(2, &[tv_xprv_1, tv_xprv_2]).as_ref().map(sh),
            Ok(tv_script_pubkey)
        );

        //     assert_eq!(
        // sortedmulti(2,xpub6ERApfZwUNrhLCkDtcHTcxd75RbzS1ed54G1LkBUHQVHQKqhMkhgbmJbZRkrgZw4koxb5JaHWkY4ALHY2grBGRjaDMzQLcgJvLJuZZvRcEL/*,xpub68NZiKmJWnxxS6aaHmn81bvJeTESw724CRDs6HbuccFQN9Ku14VQrADWgqbhhTHBaohPX4CjNLf9fq9MYo6oDaPPLPxSb7gwQN3ih19Zm4Y/0/0/*),
        //     5221025d5fc65ebb8d44a5274b53bac21ff8307fec2334a32df05553459f8b1f7fe1b62102fbd47cc8034098f0e6a94c6aeee8528abf0a2153a5d8e46d325b7284c046784652ae
        //     52210264fd4d1f5dea8ded94c61e9641309349b62f27fbffe807291f664e286bfbe6472103f4ece6dfccfa37b211eb3d0af4d0c61dba9ef698622dc17eecdf764beeb005a652ae
        //     5221022ccabda84c30bad578b13c89eb3b9544ce149787e5b538175b1d1ba259cbb83321024d902e1a2fc7a8755ab5b694c575fce742c48d9ff192e63df5193e4c7afe1f9c52ae
    }
}
