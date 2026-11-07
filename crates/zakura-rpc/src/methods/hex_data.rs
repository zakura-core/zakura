//! Deserializes hex-encoded inputs such as the one required
//! for the `submitblock` RPC method.

use std::fmt;

use hex::FromHexError;
use serde::{de, Deserializer};

#[cfg(test)]
mod tests;

/// Deserialize hex-encoded strings to bytes.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct HexData(
    #[serde(serialize_with = "hex::serialize", deserialize_with = "deserialize")] pub Vec<u8>,
);

/// Maps each ASCII byte to its hex digit value, or `u8::MAX` for a non-hex byte.
const DECODE: [u8; 256] = {
    let mut table = [u8::MAX; 256];
    let mut digit = 0;
    while digit < 16 {
        // `digit` is below 16, so it fits in a byte and the lowercase digit is ASCII.
        let lower = b"0123456789abcdef"[digit];
        table[lower as usize] = digit as u8;
        if digit >= 10 {
            table[b'A' as usize + digit - 10] = digit as u8;
        }
        digit += 1;
    }
    table
};

/// Decodes hex with a lookup table, returning the same errors as [`hex::FromHex`].
///
/// Submitted blocks are megabytes of hex, and the `hex` crate's decoder took tens of
/// milliseconds per block on the `submitblock` path.
fn decode(hex: &[u8]) -> Result<Vec<u8>, FromHexError> {
    if !hex.len().is_multiple_of(2) {
        return Err(FromHexError::OddLength);
    }

    let mut decoded = vec![0; hex.len() / 2];
    for (index, (pair, byte)) in hex.as_chunks::<2>().0.iter().zip(&mut decoded).enumerate() {
        let high = DECODE[usize::from(pair[0])];
        let low = DECODE[usize::from(pair[1])];
        if high == u8::MAX || low == u8::MAX {
            let offset = usize::from(high != u8::MAX);
            return Err(FromHexError::InvalidHexCharacter {
                c: char::from(pair[offset]),
                index: index * 2 + offset,
            });
        }
        *byte = (high << 4) | low;
    }
    Ok(decoded)
}

/// Deserializes a hex string into bytes, like `hex::deserialize`.
fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
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

    deserializer.deserialize_str(HexVisitor)
}
