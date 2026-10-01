//! Signature handling.
//!
//! This is in a separate module so we can minimize the code that has access to the internals,
//! making it easier to ensure that we check the encoding correctly.

use alloc::{borrow::ToOwned, string::String, vec::Vec};

#[cfg(feature = "signature-validation")]
use secp256k1::ecdsa;
use thiserror::Error;

use crate::script::Asm;

#[cfg(feature = "signature-validation")]
use crate::external::pubkey::PubKey;

/// Things that can go wrong when constructing a `HashType` from bit flags.
#[derive(Copy, Clone, PartialEq, Eq, Debug, Error)]
pub enum InvalidHashType {
    /// Either or both of the two least-significant bits must be set.
    #[error("invalid signed outputs")]
    InvalidSignedOutputs,
    /// With v5 transactions, bits other than those specified for `HashType` must be 0. The `i32`
    /// includes only the bits that are undefined by `HashType`.
    #[error("extra bits set")]
    ExtraBitsSet(i32),
}

/// Any error that can happen during signature decoding.
#[allow(missing_docs)]
#[derive(Copy, Clone, PartialEq, Eq, Debug, Error)]
pub enum InvalidDerInteger {
    #[error("missing the 0x02 integer encoding byte")]
    NotAnInteger,
    #[error("the integer was expected to be {expected} bytes, but it was {actual} bytes")]
    IncorrectLength { actual: usize, expected: u8 },
    #[error("integers can’t be zero-length")]
    ZeroLength,
    #[error("leading 0x00 bytes are disallowed, unless it would otherwise be interpreted as a negative number.")]
    LeadingNullByte,
    #[error("integers can’t be negative")]
    Negative,
}

/// Errors that occur during decoding of a DER signature.
#[allow(missing_docs)]
#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum InvalidDerEncoding {
    #[error("didn’t start with 0x30, or was missing the length")]
    WrongType,
    #[error("the signature can’t be longer than 70 bytes")]
    TooLong,
    #[error("the signature was expected to be {expected} bytes, but it was {actual} bytes")]
    IncorrectLength { actual: usize, expected: u8 },
    #[error("the {name} component ({value:?}) failed: {error}")]
    InvalidComponent {
        name: &'static str,
        value: Vec<u8>,
        error: InvalidDerInteger,
    },
}

/// Errors that occur when parsing signatures.
#[allow(missing_docs)]
#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum Error {
    // BIP62

    // TODO: Remove the `Option` once C++ support is removed.
    #[error(
        "{}",
        .0.map_or(
            "unknown signature hash type error".to_owned(),
            |iht| format!("signature hash type error: {iht}")
        )
    )]
    SigHashType(Option<InvalidHashType>),

    // TODO: Remove the `Option` once C++ support is removed.
    #[error(
        "{}",
        .0.clone().map_or(
            "unknown signature DER encoding error".to_owned(),
            |ide| format!("signature DER encoding error: {ide}")
        )
    )]
    SigDER(Option<InvalidDerEncoding>),

    #[error("signature s value is too high")]
    SigHighS,
}

impl From<InvalidHashType> for Error {
    fn from(value: InvalidHashType) -> Self {
        Self::SigHashType(Some(value))
    }
}

impl From<InvalidDerEncoding> for Error {
    fn from(value: InvalidDerEncoding) -> Self {
        Self::SigDER(Some(value))
    }
}

/// The ways in which a transparent input may commit to the transparent outputs of its
/// transaction.
///
/// Note that:
/// - Transparent inputs always commit to all shielded outputs.
/// - Shielded inputs always commit to all outputs.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SignedOutputs {
    /// The input signature commits to all transparent outputs in the transaction.
    All,
    /// The transparent input's signature commits to the transparent output at the same
    /// index as the transparent input.
    ///
    /// If the specified transparent output along with any shielded outputs only consume
    /// part of this input, anyone is permitted to modify the transaction to claim the
    /// remainder.
    Single,
    /// The transparent input's signature does not commit to any transparent outputs.
    ///
    /// If the shielded outputs only consume part (or none) of this input, anyone is
    /// permitted to modify the transaction to claim the remainder.
    None,
}

/// The different SigHash types, as defined in <https://zips.z.cash/zip-0143>
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct HashType {
    signed_outputs: SignedOutputs,
    anyone_can_pay: bool,
    raw_bits: i32,
}

impl HashType {
    /// Construct a `HashType` from bit flags.
    ///
    /// __TODO__: Even though the hash type is represented by a single byte, this takes `bits` as
    ///           `i32` for compatibility with the C++ API. Once that is removed, this should also
    ///           become `u8`.
    ///
    /// ## Consensus rules
    ///
    /// [§4.10](https://zips.z.cash/protocol/protocol.pdf#sighash):
    /// - Any `HashType` in a v5 transaction must have no undefined bits set.
    ///
    /// For v4 transactions and below, any value for the lower five bits other than 2 & 3 are
    /// treated as SignedOutputs::All.
    pub fn from_bits(bits: i32, is_strict: bool) -> Result<Self, InvalidHashType> {
        let unknown_bits = (bits | 0x83) ^ 0x83;
        if is_strict && unknown_bits != 0 {
            Err(InvalidHashType::ExtraBitsSet(unknown_bits))
        } else if is_strict && bits & 0x03 == 0 {
            Err(InvalidHashType::InvalidSignedOutputs)
        } else {
            Ok(HashType {
                signed_outputs: match bits & 0x1f {
                    2 => SignedOutputs::None,
                    3 => SignedOutputs::Single,
                    _ => SignedOutputs::All,
                },
                anyone_can_pay: bits & 0x80 != 0,
                raw_bits: bits,
            })
        }
    }

    /// See [SignedOutputs].
    pub fn signed_outputs(&self) -> SignedOutputs {
        self.signed_outputs
    }

    /// Allows anyone to add transparent inputs to this transaction.
    pub fn anyone_can_pay(&self) -> bool {
        self.anyone_can_pay
    }

    /// The raw hash_type bits as received from the script interpreter.
    pub fn raw_bits(&self) -> i32 {
        self.raw_bits
    }
}

impl Asm for HashType {
    fn to_asm(&self, _attempt_sighash_decode: bool) -> String {
        let signed_outputs = match self.signed_outputs {
            SignedOutputs::All => "ALL",
            SignedOutputs::Single => "SINGLE",
            SignedOutputs::None => "NONE",
        };
        let anyone_can_pay = if self.anyone_can_pay {
            "|ANYONECANPAY"
        } else {
            ""
        };
        format!("{}{}", signed_outputs, anyone_can_pay)
    }
}

impl core::fmt::Display for HashType {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.to_asm(false))
    }
}

/// Different signature encoding failures may result in either aborting execution or continuing
/// execution with an invalid signature.
pub enum Validity {
    /// Fail execution with the given error.
    InvalidAbort(Error),
    /// Continue execution, without a valid signature.
    InvalidContinue,
    /// Continue execution with a valid signature.
    Valid(Decoded),
}

#[cfg(feature = "signature-validation")]
/// This contains a validated ECDSA signature and the Zcash hash type. It’s an opaque value, so we
/// can ensure all values are valid (e.g., signature is “low-S” if required, and the hash type was
/// created without any extra bits, if required).
#[derive(Clone)]
pub struct Decoded {
    sig: ecdsa::Signature,
    hash_type: HashType,
}

/// There’s not such thing as a decoded signature unless the `signature-validation` feature is
/// enabled.
#[cfg(not(feature = "signature-validation"))]
#[derive(Clone)]
pub struct Decoded {}

#[cfg(feature = "signature-validation")]
impl Decoded {
    /// Checks the properties of individual integers in a DER signature.
    fn is_valid_integer(int_bytes: &[u8]) -> Result<(), InvalidDerInteger> {
        match int_bytes {
            [] => Err(InvalidDerInteger::ZeroLength),
            // Null bytes at the start are not allowed, unless it would otherwise be interpreted as
            // a negative number.
            [0x00, next, ..] => {
                if next & 0x80 != 0 {
                    Ok(())
                } else {
                    Err(InvalidDerInteger::LeadingNullByte)
                }
            }
            // Negative numbers are not allowed.
            [first, ..] => {
                if first & 0x80 == 0 {
                    Ok(())
                } else {
                    Err(InvalidDerInteger::Negative)
                }
            }
        }
    }

    /// A canonical signature consists of: `<30> <total len> <02> <len R> <R> <02> <len S> <S>`
    ///
    /// Where R and S are not negative (their first byte has its highest bit not set), and not
    /// excessively padded (do not start with a 0 byte, unless an otherwise negative number follows,
    /// in which case a single 0 byte is necessary and even required).
    ///
    /// See <https://bitcointalk.org/index.php?topic=8392.msg127623#msg127623>
    ///
    /// This function is consensus-critical since BIP66.
    ///
    /// __NB__: This doesn’t rely on [ecdsa::Signature::from_der] because it is consensus critical,
    ///         so we need to ensure that these exact checks happen.
    fn is_valid_encoding(sig: &[u8]) -> Result<(), InvalidDerEncoding> {
        // Format: 0x30 [total-length] 0x02 [R-length] [R] 0x02 [S-length] [S]
        // * total-length: 1-byte length descriptor of everything that follows.
        // * R-length: 1-byte length descriptor of the R value that follows.
        // * R: arbitrary-length big-endian encoded R value. It must use the shortest
        //   possible encoding for a positive integer (which means no null bytes at
        //   the start, except a single one when the next byte has its highest bit set).
        // * S-length: 1-byte length descriptor of the S value that follows.
        // * S: arbitrary-length big-endian encoded S value. The same rules apply.

        // implied checks:
        // - Minimum size constraint.
        // - Verify that the length of the signature matches the sum of the length of the elements.
        match sig {
            // A signature is of type 0x30 (compound).
            [0x30, total_len, content @ ..] => {
                // Maximum size constraint.
                if *total_len <= 70 {
                    // Make sure the length covers the entire signature.
                    if usize::from(*total_len) == content.len() {
                        match content {
                            // Check whether the R element is an integer.
                            // Extract the length of the R element.
                            [0x02, r_len, r_s @ ..] => {
                                match r_s.split_at_checked((*r_len).into()) {
                                    None => Err(InvalidDerEncoding::InvalidComponent {
                                        name: "r",
                                        value: r_s.to_vec(),
                                        error: InvalidDerInteger::IncorrectLength {
                                            actual: r_s.len(),
                                            expected: *r_len,
                                        },
                                    }),
                                    Some((r, s)) => Self::is_valid_integer(r)
                                        .map_err(|error| InvalidDerEncoding::InvalidComponent {
                                            name: "r",
                                            value: r.to_vec(),
                                            error,
                                        })
                                        .and_then(|()| match s {
                                            // Check whether the S element is an integer.
                                            // Extract the length of the S element.
                                            // Make sure the length of the S element is still inside the signature.
                                            [0x02, s_len, s @ ..] => {
                                                if usize::from(*s_len) == s.len() {
                                                    Self::is_valid_integer(s).map_err(|error| {
                                                        InvalidDerEncoding::InvalidComponent {
                                                            name: "s",
                                                            value: s.to_vec(),
                                                            error,
                                                        }
                                                    })
                                                } else {
                                                    Err(InvalidDerEncoding::InvalidComponent {
                                                        name: "s",
                                                        value: s.to_vec(),
                                                        error: InvalidDerInteger::IncorrectLength {
                                                            actual: s.len(),
                                                            expected: *s_len,
                                                        },
                                                    })
                                                }
                                            }
                                            [..] => Err(InvalidDerEncoding::InvalidComponent {
                                                name: "s",
                                                value: s.to_vec(),
                                                error: InvalidDerInteger::NotAnInteger,
                                            }),
                                        }),
                                }
                            }
                            r_s => Err(InvalidDerEncoding::InvalidComponent {
                                name: "r",
                                value: r_s.to_vec(),
                                error: InvalidDerInteger::NotAnInteger,
                            }),
                        }
                    } else {
                        Err(InvalidDerEncoding::IncorrectLength {
                            actual: content.len(),
                            expected: *total_len,
                        })
                    }
                } else {
                    Err(InvalidDerEncoding::TooLong)
                }
            }
            [..] => Err(InvalidDerEncoding::WrongType),
        }
    }

    /// This decodes an ECDSA signature and Zcash hash type from bytes. It ensures that the encoding
    /// was valid.
    ///
    /// __NB__: An empty signature is not strictly DER encoded, but will result in `Ok(None)` as a
    ///         compact way to provide an invalid signature for use with CHECK(MULTI)SIG.
    pub fn from_bytes(vch_sig_in: &[u8], require_low_s: bool, is_strict: bool) -> Validity {
        match vch_sig_in.split_last() {
            None => Validity::InvalidContinue,
            Some((hash_type, vch_sig)) => {
                let validated = Self::is_valid_encoding(vch_sig)
                    .map_err(|e| Error::SigDER(Some(e)))
                    .and_then(|()| {
                        HashType::from_bits((*hash_type).into(), is_strict)
                            .map_err(|e| Error::SigHashType(Some(e)))
                    });
                match validated {
                    Err(e) => Validity::InvalidAbort(e),
                    Ok(hash_type) => match ecdsa::Signature::from_der(vch_sig) {
                        // Failures of `ecdsa::Signature::from_der that aren’t covered by
                        // `is_valid_encoding` shouldn’t abort execution.`
                        Err(_) => Validity::InvalidContinue,
                        Ok(sig) => {
                            if require_low_s && !PubKey::check_low_s(&sig) {
                                Validity::InvalidAbort(Error::SigHighS)
                            } else {
                                Validity::Valid(Decoded { sig, hash_type })
                            }
                        }
                    },
                }
            }
        }
    }

    /// The ECDSA signature.
    pub fn sig(&self) -> &ecdsa::Signature {
        &self.sig
    }

    /// The hash type used to inform signature validation.
    pub fn sighash_type(&self) -> &HashType {
        &self.hash_type
    }
}
