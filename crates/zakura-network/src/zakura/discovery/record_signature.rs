//! The ML-DSA-65 signature of a node record, and the two extensions that
//! carry it (zakura-quic ID-7).
//!
//! A record's key list holds the author's ML-DSA-65 key as a 32-byte
//! fingerprint, and a reader verifies a relayed record without asking the
//! author for anything. So the record carries the key in full and the
//! signature, as the last two TLVs of its extension area:
//!
//! ```text
//! extension area = other TLVs | key TLV | signature TLV
//! key TLV        = 0x0001 (u16 LE) | 1952 (u16 LE) | ML-DSA-65 public key
//! signature TLV  = 0x0002 (u16 LE) | 3309 (u16 LE) | ML-DSA-65 signature
//! ```
//!
//! The ML-DSA-65 signature covers the record's signing domain, its format
//! version, and every body byte before the signature TLV: the fields, the
//! other TLVs and the key TLV. The Ed25519 signature covers the same bytes
//! and the signature TLV after them, so it also fixes which ML-DSA signature
//! the record carries.
//!
//! The encoding is canonical. The area is whole TLVs, each of the two types
//! appears exactly once, with its scheme's exact length, and nothing follows
//! the signature TLV. A TLV of any other type is the author's to define: it
//! is kept byte for byte, relayed, and covered by both signatures.

use thiserror::Error;
use zakura_quic::{
    verify_record_ml_dsa_65, NodeKeyList, NodeSecretKey, SignatureError, ML_DSA_65_KEY_BYTES,
    ML_DSA_65_KEY_TAG, ML_DSA_65_SIGNATURE_BYTES,
};

/// The header of one extension in a record's extension area: a `u16` type
/// and a `u16` length.
pub(super) const RECORD_EXTENSION_HEADER_BYTES: usize = 2 + 2;

/// The type of the extension that holds the author's ML-DSA-65 public key.
pub const RECORD_EXTENSION_ML_DSA_65_KEY: u16 = 1;

/// The type of the extension that holds the ML-DSA-65 record signature.
pub const RECORD_EXTENSION_ML_DSA_65_SIGNATURE: u16 = 2;

/// Bytes of the key TLV.
const KEY_TLV_BYTES: usize = RECORD_EXTENSION_HEADER_BYTES + ML_DSA_65_KEY_BYTES;

/// Bytes of the signature TLV, which the ML-DSA-65 signature does not cover.
const SIGNATURE_TLV_BYTES: usize = RECORD_EXTENSION_HEADER_BYTES + ML_DSA_65_SIGNATURE_BYTES;

/// Bytes of a record's extension area that its ML-DSA-65 key and signature
/// take: 5,269.
pub const RECORD_ML_DSA_65_EXTENSION_BYTES: usize = KEY_TLV_BYTES + SIGNATURE_TLV_BYTES;

/// Why a record's extension area does not hold a valid ML-DSA-65 signature.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RecordSignatureError {
    /// The area is not a sequence of whole TLVs.
    #[error("the extension area is not whole TLVs")]
    Malformed,
    /// The area has no ML-DSA-65 key TLV.
    #[error("the record carries no ML-DSA-65 key")]
    MissingKey,
    /// The area has no ML-DSA-65 signature TLV.
    #[error("the record carries no ML-DSA-65 signature")]
    MissingSignature,
    /// The key or the signature TLV appears more than once.
    #[error("the record repeats its ML-DSA-65 key or signature")]
    Duplicate,
    /// The key and signature TLVs are not the last two of the area, in that
    /// order.
    #[error("the ML-DSA-65 key and signature do not end the extension area")]
    Misplaced,
    /// The key or the signature TLV does not have its scheme's length.
    #[error("the ML-DSA-65 key or signature has the wrong length")]
    Length,
    /// The key is not the one the record's key list names, or the signature
    /// does not verify with it.
    #[error("the ML-DSA-65 record signature does not verify")]
    Invalid,
}

/// A record's extension area, split at its ML-DSA-65 key and signature.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct SignatureExtensions<'a> {
    /// The TLVs before the key TLV, none of them a key or signature TLV.
    pub(super) other: &'a [u8],
    /// The author's ML-DSA-65 public key.
    pub(super) key: &'a [u8],
    /// The ML-DSA-65 signature.
    pub(super) signature: &'a [u8],
}

/// One TLV's type and value length, as its header gives them.
#[derive(Clone, Copy)]
struct Header {
    kind: u16,
    len: usize,
}

/// What one pass over an extension area of whole TLVs found.
struct Scan {
    /// How many key TLVs the area holds.
    keys: usize,
    /// How many signature TLVs the area holds.
    signatures: usize,
    /// The last two TLVs of the area, if it has that many.
    last_two: Option<(Header, Header)>,
}

/// Reads every TLV header of `area`, or fails when the area is not whole
/// TLVs. `area` is untrusted: it comes from a peer or the book file.
fn scan(area: &[u8]) -> Result<Scan, RecordSignatureError> {
    let (mut keys, mut signatures) = (0, 0);
    let (mut previous, mut last) = (None, None);
    let mut rest = area;
    // Each pass consumes at least a header, so the body cap bounds the loop.
    while !rest.is_empty() {
        let (header, tail) = rest
            .split_first_chunk::<RECORD_EXTENSION_HEADER_BYTES>()
            .ok_or(RecordSignatureError::Malformed)?;
        let header = Header {
            kind: u16::from_le_bytes([header[0], header[1]]),
            len: usize::from(u16::from_le_bytes([header[2], header[3]])),
        };
        let (_, tail) = tail
            .split_at_checked(header.len)
            .ok_or(RecordSignatureError::Malformed)?;
        match header.kind {
            RECORD_EXTENSION_ML_DSA_65_KEY => keys += 1,
            RECORD_EXTENSION_ML_DSA_65_SIGNATURE => signatures += 1,
            _ => {}
        }
        (previous, last) = (last, Some(header));
        rest = tail;
    }
    Ok(Scan {
        keys,
        signatures,
        last_two: previous.zip(last),
    })
}

/// Splits `area` under the module's canonical encoding, or says which rule
/// it breaks. `area` is untrusted: it comes from a peer or the book file.
pub(super) fn parse(area: &[u8]) -> Result<SignatureExtensions<'_>, RecordSignatureError> {
    let Scan {
        keys,
        signatures,
        last_two,
    } = scan(area)?;
    if keys == 0 {
        return Err(RecordSignatureError::MissingKey);
    }
    if signatures == 0 {
        return Err(RecordSignatureError::MissingSignature);
    }
    if keys > 1 || signatures > 1 {
        return Err(RecordSignatureError::Duplicate);
    }
    // The area holds one TLV of each type, so it has a last two.
    let (key, signature) = last_two.ok_or(RecordSignatureError::Misplaced)?;
    if key.kind != RECORD_EXTENSION_ML_DSA_65_KEY
        || signature.kind != RECORD_EXTENSION_ML_DSA_65_SIGNATURE
    {
        return Err(RecordSignatureError::Misplaced);
    }
    if key.len != ML_DSA_65_KEY_BYTES || signature.len != ML_DSA_65_SIGNATURE_BYTES {
        return Err(RecordSignatureError::Length);
    }

    // The loop read both TLVs whole at the end of `area`, with these lengths.
    let (other, pair) = area.split_at(area.len() - RECORD_ML_DSA_65_EXTENSION_BYTES);
    let (key, signature) = pair.split_at(KEY_TLV_BYTES);
    Ok(SignatureExtensions {
        other,
        key: &key[RECORD_EXTENSION_HEADER_BYTES..],
        signature: &signature[RECORD_EXTENSION_HEADER_BYTES..],
    })
}

/// The part of a record's Ed25519 pre-image that its ML-DSA-65 signature
/// covers: everything before the signature TLV, which ends the body.
///
/// `pre_image` is the pre-image of a body whose extension area [`parse`]
/// accepted or [`write_unsigned`] built, so it ends in a whole signature TLV.
fn signed_bytes(pre_image: &[u8]) -> &[u8] {
    &pre_image[..pre_image.len() - SIGNATURE_TLV_BYTES]
}

/// Verifies the ML-DSA-65 signature of the record whose key list is `keys`,
/// whose extension area is `area`, and whose Ed25519 pre-image is
/// `pre_image`, which therefore ends in `area`.
pub(super) fn verify(
    keys: &NodeKeyList,
    area: &[u8],
    pre_image: &[u8],
) -> Result<(), RecordSignatureError> {
    debug_assert!(pre_image.ends_with(area));
    let extensions = parse(area)?;
    verify_record_ml_dsa_65(
        keys,
        extensions.key,
        signed_bytes(pre_image),
        extensions.signature,
    )
    .map_err(|_| RecordSignatureError::Invalid)
}

/// Makes `area` the extension area of a record `signer` is about to sign:
/// the author's other TLVs, the key TLV, and a signature TLV whose value is
/// still zero.
///
/// `area` holds the author's other TLVs on entry. It may also be the area of
/// an earlier record by `signer`, whose key and signature TLVs are then
/// replaced. Fails when the other TLVs are not whole, or hold a key or
/// signature TLV, because no reader would accept the record.
pub(super) fn write_unsigned(
    area: &mut Vec<u8>,
    signer: &NodeSecretKey,
) -> Result<(), RecordSignatureError> {
    if let Ok(SignatureExtensions { other, .. }) = parse(area) {
        area.truncate(other.len());
    }
    let others = scan(area)?;
    if others.keys != 0 || others.signatures != 0 {
        return Err(RecordSignatureError::Duplicate);
    }

    let key = signer
        .credential()
        .key(ML_DSA_65_KEY_TAG)
        .expect("a secret key's credential always holds its ML-DSA-65 key in full");
    let header = |kind: u16, len: usize| {
        let len = u16::try_from(len).expect("an ML-DSA-65 key or signature is under 64 KiB");
        [kind.to_le_bytes(), len.to_le_bytes()].concat()
    };
    area.reserve(RECORD_ML_DSA_65_EXTENSION_BYTES);
    area.extend_from_slice(&header(RECORD_EXTENSION_ML_DSA_65_KEY, key.len()));
    area.extend_from_slice(key);
    area.extend_from_slice(&header(
        RECORD_EXTENSION_ML_DSA_65_SIGNATURE,
        ML_DSA_65_SIGNATURE_BYTES,
    ));
    area.resize(area.len() + ML_DSA_65_SIGNATURE_BYTES, 0);
    Ok(())
}

/// Signs the record whose extension area is `area`, as [`write_unsigned`]
/// built it, and whose Ed25519 pre-image is `pre_image`, which therefore
/// ends in `area`: writes the ML-DSA-65 signature, hedged with `randomness`,
/// into the signature TLV of each. Fails only if ML-DSA gives up signing,
/// which no honest input causes in practice.
pub(super) fn sign(
    area: &mut [u8],
    pre_image: &mut [u8],
    signer: &NodeSecretKey,
    randomness: [u8; 32],
) -> Result<(), SignatureError> {
    debug_assert!(pre_image.ends_with(area));
    let signature = signer.sign_record_ml_dsa_65(signed_bytes(pre_image), randomness)?;
    for bytes in [area, pre_image] {
        let value_at = bytes.len() - ML_DSA_65_SIGNATURE_BYTES;
        bytes[value_at..].copy_from_slice(&signature);
    }
    Ok(())
}

#[cfg(any(test, feature = "p2p-fuzz"))]
pub(super) mod fuzz;
#[cfg(test)]
mod tests;
