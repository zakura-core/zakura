//! Hex encoding for byte payloads in RPC requests and responses, such as the
//! blocks in `submitblock` and the transactions in `getblocktemplate`.
//!
//! Payloads are megabytes of hex, and the `hex` crate's byte-at-a-time codec
//! took milliseconds per block. This module uses the SIMD codec from
//! `const-hex`, while keeping the inputs it accepts and the errors it reports
//! identical to the `hex` crate's.
//!
//! Use it with `#[serde(with = "...::hex_data")]` only for types whose hex form
//! is their raw bytes in order. Hashes display reversed bytes through their
//! `hex::ToHex` implementations, so they keep `#[serde(with = "hex")]`.

use std::fmt;

use hex::FromHexError;
use serde::{de, ser, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

use crate::server::jsonrpc::is_writing_result;

#[cfg(test)]
mod tests;

/// Deserialize hex-encoded strings to bytes.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct HexData(
    #[serde(serialize_with = "serialize", deserialize_with = "deserialize")] pub Vec<u8>,
);

/// Encodes bytes as lowercase hex, like `hex::encode`.
pub(crate) fn encode(bytes: impl AsRef<[u8]>) -> String {
    const_hex::encode(bytes)
}

/// Decodes upper or lower case hex, accepting and rejecting exactly the inputs
/// `hex::FromHex` does, with the same errors.
pub(crate) fn decode(hex: &[u8]) -> Result<Vec<u8>, FromHexError> {
    // `const-hex` strips an optional `0x` prefix, which the `hex` crate rejects.
    // Report the error `hex` does: an odd length first, then the `x`.
    if hex.len().is_multiple_of(2) && matches!(hex, [b'0', b'x' | b'X', ..]) {
        return Err(FromHexError::InvalidHexCharacter {
            c: char::from(hex[1]),
            index: 1,
        });
    }
    const_hex::decode(hex).map_err(|error| match error {
        const_hex::FromHexError::InvalidHexCharacter { c, index } => {
            FromHexError::InvalidHexCharacter { c, index }
        }
        const_hex::FromHexError::OddLength => FromHexError::OddLength,
        const_hex::FromHexError::InvalidStringLength => FromHexError::InvalidStringLength,
    })
}

/// Serializes bytes as a hex string, like `hex::serialize`.
///
/// In a method's response, the string is written as finished JSON. Hex has no characters that
/// JSON escapes, and looking for them one byte at a time took as long as the rest of a full block
/// template's encoding.
pub(crate) fn serialize<S, T>(bytes: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: AsRef<[u8]>,
{
    if !is_writing_result() {
        return serializer.serialize_str(&encode(bytes));
    }

    let bytes = bytes.as_ref();
    let mut json = vec![b'"'; bytes.len() * 2 + 2];
    let hex = json
        .get_mut(1..=bytes.len() * 2)
        .expect("the buffer holds the hex between two quotes");
    const_hex::encode_to_slice(bytes, hex).map_err(ser::Error::custom)?;

    let json = std::str::from_utf8(&json).map_err(ser::Error::custom)?;
    serde_json::from_str::<&RawValue>(json)
        .map_err(ser::Error::custom)?
        .serialize(serializer)
}

/// Deserializes a hex string into bytes, like `hex::deserialize`.
pub(crate) fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: From<Vec<u8>>,
{
    struct HexVisitor;

    impl de::Visitor<'_> for HexVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a hex encoded string")
        }

        fn visit_str<E: de::Error>(self, data: &str) -> Result<Vec<u8>, E> {
            decode(data.as_bytes()).map_err(E::custom)
        }
    }

    deserializer.deserialize_str(HexVisitor).map(T::from)
}
