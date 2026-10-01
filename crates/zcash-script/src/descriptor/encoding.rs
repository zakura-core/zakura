use core::fmt;
use core::str::FromStr;

use bip32::ExtendedKey;
use thiserror::Error;

use super::{Key, KeyExpression, KeyOrigin};

impl FromStr for KeyExpression {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(pos) = s.find(']') {
            let (origin, key) = s.split_at(pos + 1);
            Ok(Self {
                origin: Some(origin.parse()?),
                key: key.parse()?,
            })
        } else {
            Ok(Self {
                origin: None,
                key: s.parse()?,
            })
        }
    }
}

impl fmt::Display for KeyExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(origin) = &self.origin {
            origin.fmt(f)?;
        }
        self.key.fmt(f)
    }
}

impl FromStr for KeyOrigin {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .ok_or(ParseError::InvalidKeyOrigin)
            .and_then(|s| {
                let mut parts = s.split('/');

                let fingerprint = hex::decode(
                    parts
                        .next()
                        .expect("split() returns the full input on no matches"),
                )
                .ok()
                .and_then(|fingerprint| <[u8; 4]>::try_from(fingerprint).ok())
                .ok_or(ParseError::InvalidFingerprint)?;

                let derivation = parts
                    .map(|part| {
                        if let Some(part) = part.strip_suffix(['\'', 'h']) {
                            bip32::ChildNumber::new(part.parse().ok()?, true)
                        } else {
                            bip32::ChildNumber::new(part.parse().ok()?, false)
                        }
                        .ok()
                    })
                    .collect::<Option<_>>()
                    .ok_or(ParseError::InvalidPathElement)?;

                Ok(KeyOrigin {
                    fingerprint,
                    derivation,
                })
            })
    }
}

impl fmt::Display for KeyOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}", hex::encode(self.fingerprint))?;
        for i in &self.derivation {
            write!(
                f,
                "/{}{}",
                i.index(),
                if i.is_hardened() { "'" } else { "" },
            )?;
        }
        write!(f, "]")
    }
}

impl FromStr for Key {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(key) = s.parse() {
            // Hex-encoded pubkey bytes.
            Ok(Key::Public {
                key,
                compressed: s.len() == 66,
            })
        } else {
            // Extended key with optional child derivation.
            let mut parts = s.split('/');

            let key = parts
                .next()
                .expect("split() returns the full input on no matches")
                .parse::<ExtendedKey>()
                .map_err(|_| ParseError::InvalidKey)?;

            let derivation = parts
                .map(|part| {
                    let (index, hardened) = if let Some(index) = part.strip_suffix(['\'', 'h']) {
                        (index, true)
                    } else {
                        (part, false)
                    };

                    if index == "*" {
                        Err(ParseError::MultipleLeavesUnsupported)
                    } else {
                        bip32::ChildNumber::new(
                            index.parse().map_err(|_| ParseError::InvalidPathElement)?,
                            hardened,
                        )
                        .map_err(|_| ParseError::InvalidPathElement)
                    }
                })
                .collect::<Result<_, _>>()?;

            let prefix = key.prefix;

            if prefix.is_public() {
                Key::checked_xpub(
                    prefix,
                    key.try_into().map_err(|_| ParseError::InvalidKey)?,
                    derivation,
                )
                .ok_or(ParseError::InvalidPathElement)
            } else if prefix.is_private() {
                Err(ParseError::PrivateKeysUnsupported)
            } else {
                Err(ParseError::InvalidKey)
            }
        }
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Key::Public { key, compressed } => {
                write!(
                    f,
                    "{}",
                    if *compressed {
                        hex::encode(key.serialize())
                    } else {
                        hex::encode(key.serialize_uncompressed())
                    }
                )
            }
            Key::Xpub { prefix, key, child } => {
                write!(f, "{}", key.to_string(*prefix))?;
                for i in child {
                    write!(f, "/{}", i.index(),)?;
                }
                Ok(())
            }
        }
    }
}

/// Errors that can happen when parsing Key Expressions.
#[allow(missing_docs)]
#[derive(Copy, Clone, PartialEq, Eq, Debug, Error)]
pub enum ParseError {
    #[error("key expression contained an invalid key origin")]
    InvalidKeyOrigin,
    #[error("the key fingerprint was not exactly 8 hex characters")]
    InvalidFingerprint,
    #[error("key origin contained an invalid derivation path element")]
    InvalidPathElement,
    #[error("key expression contained an invalid key")]
    InvalidKey,
    #[error("key expressions ending in /* or /*' are currently unsupported")]
    MultipleLeavesUnsupported,
    #[error("key expressions containing private keys are currently unsupported")]
    PrivateKeysUnsupported,
}

#[cfg(test)]
mod tests {
    use crate::descriptor::KeyExpression;

    /// From https://github.com/bitcoin/bips/blob/master/bip-0380.mediawiki#test-vectors
    #[test]
    fn bip_380_key_expression_test_vectors() {
        const VALID_EXPRESSIONS: &[&str] = &[
            // Compressed public key
            "0260b2003c386519fc9eadf2b5cf124dd8eea4c4e68d5e154050a9346ea98ce600",
            // Uncompressed public key
            "04a34b99f22c790c4e36b2b3c2c35a36db06226e41c692fc82b8b56ac1c540c5bd5b8dec5235a0fa8722476c7709c02559e3aa73aa03918ba2d492eea75abea235",
            // Public key with key origin
            "[deadbeef/0h/0h/0h]0260b2003c386519fc9eadf2b5cf124dd8eea4c4e68d5e154050a9346ea98ce600",
            // Public key with key origin (' as hardened indicator)
            "[deadbeef/0'/0'/0']0260b2003c386519fc9eadf2b5cf124dd8eea4c4e68d5e154050a9346ea98ce600",
            // Public key with key origin (mixed hardened indicator)
            "[deadbeef/0'/0h/0']0260b2003c386519fc9eadf2b5cf124dd8eea4c4e68d5e154050a9346ea98ce600",
            // // WIF uncompressed private key
            // "5KYZdUEo39z3FPrtuX2QbbwGnNP5zTd7yyr2SC1j299sBCnWjss",
            // // WIF compressed private key
            // "L4rK1yDtCWekvXuE6oXD9jCYfFNV2cWRpVuPLBcCU2z8TrisoyY1",
            // Extended public key
            "xpub6ERApfZwUNrhLCkDtcHTcxd75RbzS1ed54G1LkBUHQVHQKqhMkhgbmJbZRkrgZw4koxb5JaHWkY4ALHY2grBGRjaDMzQLcgJvLJuZZvRcEL",
            // Extended public key with key origin
            "[deadbeef/0h/1h/2h]xpub6ERApfZwUNrhLCkDtcHTcxd75RbzS1ed54G1LkBUHQVHQKqhMkhgbmJbZRkrgZw4koxb5JaHWkY4ALHY2grBGRjaDMzQLcgJvLJuZZvRcEL",
            // Extended public key with derivation
            "[deadbeef/0h/1h/2h]xpub6ERApfZwUNrhLCkDtcHTcxd75RbzS1ed54G1LkBUHQVHQKqhMkhgbmJbZRkrgZw4koxb5JaHWkY4ALHY2grBGRjaDMzQLcgJvLJuZZvRcEL/3/4/5",
            // // Extended public key with derivation and children
            // "[deadbeef/0h/1h/2h]xpub6ERApfZwUNrhLCkDtcHTcxd75RbzS1ed54G1LkBUHQVHQKqhMkhgbmJbZRkrgZw4koxb5JaHWkY4ALHY2grBGRjaDMzQLcgJvLJuZZvRcEL/3/4/5/*",
            // // Extended public key with hardened derivation and unhardened children
            // "xpub6ERApfZwUNrhLCkDtcHTcxd75RbzS1ed54G1LkBUHQVHQKqhMkhgbmJbZRkrgZw4koxb5JaHWkY4ALHY2grBGRjaDMzQLcgJvLJuZZvRcEL/3h/4h/5h/*",
            // // Extended public key with hardened derivation and children
            // "xpub6ERApfZwUNrhLCkDtcHTcxd75RbzS1ed54G1LkBUHQVHQKqhMkhgbmJbZRkrgZw4koxb5JaHWkY4ALHY2grBGRjaDMzQLcgJvLJuZZvRcEL/3h/4h/5h/*h",
            // // Extended public key with key origin, hardened derivation and children
            // "[deadbeef/0h/1h/2]xpub6ERApfZwUNrhLCkDtcHTcxd75RbzS1ed54G1LkBUHQVHQKqhMkhgbmJbZRkrgZw4koxb5JaHWkY4ALHY2grBGRjaDMzQLcgJvLJuZZvRcEL/3h/4h/5h/*h",
            // // Extended private key
            // "xprvA1RpRA33e1JQ7ifknakTFpgNXPmW2YvmhqLQYMmrj4xJXXWYpDPS3xz7iAxn8L39njGVyuoseXzU6rcxFLJ8HFsTjSyQbLYnMpCqE2VbFWc",
            // // Extended private key with key origin
            // "[deadbeef/0h/1h/2h]xprvA1RpRA33e1JQ7ifknakTFpgNXPmW2YvmhqLQYMmrj4xJXXWYpDPS3xz7iAxn8L39njGVyuoseXzU6rcxFLJ8HFsTjSyQbLYnMpCqE2VbFWc",
            // // Extended private key with derivation
            // "[deadbeef/0h/1h/2h]xprvA1RpRA33e1JQ7ifknakTFpgNXPmW2YvmhqLQYMmrj4xJXXWYpDPS3xz7iAxn8L39njGVyuoseXzU6rcxFLJ8HFsTjSyQbLYnMpCqE2VbFWc/3/4/5",
            // // Extended private key with derivation and children
            // "[deadbeef/0h/1h/2h]xprvA1RpRA33e1JQ7ifknakTFpgNXPmW2YvmhqLQYMmrj4xJXXWYpDPS3xz7iAxn8L39njGVyuoseXzU6rcxFLJ8HFsTjSyQbLYnMpCqE2VbFWc/3/4/5/*",
            // // Extended private key with hardened derivation and unhardened children
            // "xprvA1RpRA33e1JQ7ifknakTFpgNXPmW2YvmhqLQYMmrj4xJXXWYpDPS3xz7iAxn8L39njGVyuoseXzU6rcxFLJ8HFsTjSyQbLYnMpCqE2VbFWc/3h/4h/5h/*",
            // // Extended private key with hardened derivation and children
            // "xprvA1RpRA33e1JQ7ifknakTFpgNXPmW2YvmhqLQYMmrj4xJXXWYpDPS3xz7iAxn8L39njGVyuoseXzU6rcxFLJ8HFsTjSyQbLYnMpCqE2VbFWc/3h/4h/5h/*h",
            // // Extended private key with key origin, hardened derivation and children
            // "[deadbeef/0h/1h/2]xprvA1RpRA33e1JQ7ifknakTFpgNXPmW2YvmhqLQYMmrj4xJXXWYpDPS3xz7iAxn8L39njGVyuoseXzU6rcxFLJ8HFsTjSyQbLYnMpCqE2VbFWc/3h/4h/5h/*h",
        ];

        const INVALID_EXPRESSIONS: &[&str] = &[
            // Children indicator in key origin
            "[deadbeef/0h/0h/0h/*]0260b2003c386519fc9eadf2b5cf124dd8eea4c4e68d5e154050a9346ea98ce600",
            // Trailing slash in key origin
            "[deadbeef/0h/0h/0h/]0260b2003c386519fc9eadf2b5cf124dd8eea4c4e68d5e154050a9346ea98ce600",
            // Too short fingerprint
            "[deadbef/0h/0h/0h]0260b2003c386519fc9eadf2b5cf124dd8eea4c4e68d5e154050a9346ea98ce600",
            // Too long fingerprint
            "[deadbeeef/0h/0h/0h]0260b2003c386519fc9eadf2b5cf124dd8eea4c4e68d5e154050a9346ea98ce600",
            // Invalid hardened indicators
            "[deadbeef/0f/0f/0f]0260b2003c386519fc9eadf2b5cf124dd8eea4c4e68d5e154050a9346ea98ce600",
            // Invalid hardened indicators
            "[deadbeef/-0/-0/-0]0260b2003c386519fc9eadf2b5cf124dd8eea4c4e68d5e154050a9346ea98ce600",
            // Invalid hardened indicators
            "[deadbeef/0H/0H/0H]0260b2003c386519fc9eadf2b5cf124dd8eea4c4e68d5e154050a9346ea98ce600",
            // Invalid hardened indicators
            "[deadbeef/0h/1h/2]xprvA1RpRA33e1JQ7ifknakTFpgNXPmW2YvmhqLQYMmrj4xJXXWYpDPS3xz7iAxn8L39njGVyuoseXzU6rcxFLJ8HFsTjSyQbLYnMpCqE2VbFWc/3H/4h/5h/*H",
            // Private key with derivation
            "L4rK1yDtCWekvXuE6oXD9jCYfFNV2cWRpVuPLBcCU2z8TrisoyY1/0",
            // Private key with derivation children
            "L4rK1yDtCWekvXuE6oXD9jCYfFNV2cWRpVuPLBcCU2z8TrisoyY1/*",
            // Derivation index out of range
            "xprv9s21ZrQH143K31xYSDQpPDxsXRTUcvj2iNHm5NUtrGiGG5e2DtALGdso3pGz6ssrdK4PFmM8NSpSBHNqPqm55Qn3LqFtT2emdEXVYsCzC2U/2147483648",
            // Invalid derivation index
            "xprv9s21ZrQH143K31xYSDQpPDxsXRTUcvj2iNHm5NUtrGiGG5e2DtALGdso3pGz6ssrdK4PFmM8NSpSBHNqPqm55Qn3LqFtT2emdEXVYsCzC2U/1aa",
            // Multiple key origins
            "[aaaaaaaa][aaaaaaaa]xprv9s21ZrQH143K31xYSDQpPDxsXRTUcvj2iNHm5NUtrGiGG5e2DtALGdso3pGz6ssrdK4PFmM8NSpSBHNqPqm55Qn3LqFtT2emdEXVYsCzC2U/2147483647'/0",
            // Missing key origin start
            "aaaaaaaa]xprv9s21ZrQH143K31xYSDQpPDxsXRTUcvj2iNHm5NUtrGiGG5e2DtALGdso3pGz6ssrdK4PFmM8NSpSBHNqPqm55Qn3LqFtT2emdEXVYsCzC2U/2147483647'/0",
            // Non hex fingerprint
            "[gaaaaaaa]xprv9s21ZrQH143K31xYSDQpPDxsXRTUcvj2iNHm5NUtrGiGG5e2DtALGdso3pGz6ssrdK4PFmM8NSpSBHNqPqm55Qn3LqFtT2emdEXVYsCzC2U/2147483647'/0",
            // Key origin with no public key
            "[deadbeef]",
        ];

        for s in VALID_EXPRESSIONS {
            assert!(
                s.parse::<KeyExpression>().is_ok(),
                "rejected valid key expression {s}",
            );
        }

        for s in INVALID_EXPRESSIONS {
            assert!(
                s.parse::<KeyExpression>().is_err(),
                "allowed invalid key expression {s}",
            );
        }
    }
}
