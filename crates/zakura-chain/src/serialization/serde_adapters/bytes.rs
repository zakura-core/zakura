//! Fixed byte arrays encoded as tuples, without a binary length prefix.

use std::fmt;

use serde::{
    de::{Error, SeqAccess, Visitor},
    ser::SerializeTuple,
    Deserializer, Serializer,
};

/// Serializes exactly `N` bytes using [`Serializer::serialize_tuple`].
pub fn serialize<S: Serializer, const N: usize>(
    bytes: &[u8; N],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut tuple = serializer.serialize_tuple(N)?;
    for byte in bytes {
        tuple.serialize_element(byte)?;
    }
    tuple.end()
}

/// Deserializes exactly `N` bytes into an initialized fixed-size array.
pub fn deserialize<'de, D: Deserializer<'de>, const N: usize>(
    deserializer: D,
) -> Result<[u8; N], D::Error> {
    struct ByteArrayVisitor<const N: usize>;

    impl<'de, const N: usize> Visitor<'de> for ByteArrayVisitor<N> {
        type Value = [u8; N];

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "an array of length {N}")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
            let mut bytes = [0; N];
            for (index, byte) in bytes.iter_mut().enumerate() {
                *byte = sequence
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(index, &self))?;
            }
            Ok(bytes)
        }
    }

    deserializer.deserialize_tuple(N, ByteArrayVisitor::<N>)
}
