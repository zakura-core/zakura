//! Converting bytes into Zcash consensus-critical data structures.

use std::io::Read as _;
use std::{io, net::Ipv6Addr, sync::Arc};

use super::{
    AtLeastOne, CompactSizeMessage, SerializationError, ZcashReader, MAX_PROTOCOL_MESSAGE_LEN,
};

/// Initial-allocation cap for `zcash_deserialize_external_count`.
///
/// 1024 is large enough that honest messages amortize their growth to a few
/// reallocations.
pub(crate) const MAX_INITIAL_ALLOCATION: usize = 1024;

/// Consensus-critical deserialization for Zcash.
///
/// This trait provides a generic deserialization for consensus-critical
/// formats, such as network messages, transactions, blocks, etc.
///
/// It is intended for use only for consensus-critical formats.
/// Internal deserialization can freely use `serde`, or any other format.
pub trait ZcashDeserialize: Sized {
    /// Try to read `self` from the given `reader`.
    ///
    /// This function has a `zcash_` prefix to alert the reader that the
    /// serialization in use is consensus-critical serialization, rather than
    /// some other kind of serialization.
    fn zcash_deserialize<R: io::Read>(reader: R) -> Result<Self, SerializationError> {
        Self::zcash_deserialize_from(&mut ZcashReader::from_stream(reader))
    }

    /// Decode from actual payload bytes, preserving allocation bounds through
    /// nested values. Advances the slice by the bytes consumed.
    fn zcash_deserialize_from_slice(bytes: &mut &[u8]) -> Result<Self, SerializationError> {
        Self::zcash_deserialize_from(&mut ZcashReader::from_slice(bytes))
    }

    /// Decode one value using the supplied input bounds. Implementations must
    /// use `read_value`, `read_external_count`, and `read_bytes` for nested data.
    /// The default rejects types that have only a streaming decoder.
    fn zcash_deserialize_from<R: io::Read>(
        _reader: &mut ZcashReader<R>,
    ) -> Result<Self, SerializationError> {
        Err(SerializationError::Parse("type has no bounded decoder"))
    }
}

/// Deserialize a `Vec`, where the number of items is set by a CompactSize
/// prefix in the data. This is the most common format in Zcash.
///
/// See `zcash_deserialize_external_count` for more details, and usage
/// information.
impl<T: ZcashDeserialize + TrustedPreallocate> ZcashDeserialize for Vec<T> {
    fn zcash_deserialize_from<R: io::Read>(
        reader: &mut ZcashReader<R>,
    ) -> Result<Self, SerializationError> {
        let len: CompactSizeMessage = reader.read_value()?;
        reader.read_external_count(len.into())
    }
}

/// Deserialize an `AtLeastOne` vector, where the number of items is set by a
/// CompactSize prefix in the data. This is the most common format in Zcash.
impl<T: ZcashDeserialize + TrustedPreallocate> ZcashDeserialize for AtLeastOne<T> {
    fn zcash_deserialize_from<R: io::Read>(
        reader: &mut ZcashReader<R>,
    ) -> Result<Self, SerializationError> {
        let v: Vec<T> = reader.read_value()?;
        let at_least_one: AtLeastOne<T> = v.try_into()?;
        Ok(at_least_one)
    }
}

/// Implement ZcashDeserialize for `Vec<u8>` directly instead of using the blanket Vec implementation
///
/// This allows us to optimize the inner loop into a small number of `read_exact()`
/// calls, rather than one call per byte.
/// Note that we don't implement TrustedPreallocate for u8.
/// This allows the optimization without relying on specialization.
impl ZcashDeserialize for Vec<u8> {
    fn zcash_deserialize_from<R: io::Read>(
        reader: &mut ZcashReader<R>,
    ) -> Result<Self, SerializationError> {
        let len: CompactSizeMessage = reader.read_value()?;
        reader.read_bytes(len.into())
    }
}

/// Deserialize a `Vec` containing `external_count` items.
///
/// In Zcash, most arrays are stored as a CompactSize, followed by that number
/// of items of type `T`. But in `Transaction::V5`, some types are serialized as
/// multiple arrays in different locations, with a single CompactSize before the
/// first array.
///
/// ## Usage
///
/// Use `zcash_deserialize_external_count` when the array count is determined by
/// other data, or a consensus rule.
///
/// Use `Vec::zcash_deserialize` for data that contains CompactSize count,
/// followed by the data array.
///
/// For example, when a single count applies to multiple arrays:
/// 1. Use `Vec::zcash_deserialize` for the array that has a data count.
/// 2. Use `zcash_deserialize_external_count` for the arrays with no count in the
///    data, passing the length of the first array.
///
/// This function has a `zcash_` prefix to alert the reader that the
/// serialization in use is consensus-critical serialization, rather than
/// some other kind of serialization.
pub fn zcash_deserialize_external_count<R: io::Read, T: ZcashDeserialize + TrustedPreallocate>(
    external_count: usize,
    reader: R,
) -> Result<Vec<T>, SerializationError> {
    ZcashReader::from_stream(reader).read_external_count(external_count)
}

pub(super) fn read_external_count<R: io::Read, T: ZcashDeserialize + TrustedPreallocate>(
    external_count: usize,
    reader: &mut ZcashReader<R>,
) -> Result<Vec<T>, SerializationError> {
    match u64::try_from(external_count) {
        Ok(external_count) if external_count > T::max_allocation() => {
            return Err(SerializationError::Parse(
                "Vector longer than max_allocation",
            ))
        }
        Ok(_) => {}
        // As of 2021, usize is less than or equal to 64 bits on all (or almost all?) supported Rust platforms.
        // So in practice this error is impossible. (But the check is required, because Rust is future-proof
        // for 128 bit memory spaces.)
        Err(_) => return Err(SerializationError::Parse("Vector longer than u64::MAX")),
    }
    let initial_capacity = if let Some(remaining) = reader.remaining_bytes() {
        let minimum = T::min_serialized_size();
        if external_count != 0
            && (minimum == 0
                || u64::try_from(external_count).unwrap_or(u64::MAX)
                    > u64::try_from(remaining).unwrap_or(u64::MAX) / minimum)
        {
            return Err(SerializationError::Parse("Vector exceeds available input"));
        }
        external_count
    } else {
        external_count.min(MAX_INITIAL_ALLOCATION)
    };

    let mut vec = Vec::with_capacity(initial_capacity);
    for _ in 0..external_count {
        let item = reader.read_value()?;
        reserve_bounded(&mut vec, 1, external_count);
        vec.push(item);
    }
    Ok(vec)
}

/// `zcash_deserialize_external_count`, specialised for raw bytes.
///
/// This reads in chunks, so the inner loop is a small number of `read_exact()`
/// calls rather than one call per byte.
///
/// This function has a `zcash_` prefix to alert the reader that the
/// serialization in use is consensus-critical serialization, rather than
/// some other kind of serialization.
pub fn zcash_deserialize_bytes_external_count<R: io::Read>(
    external_count: usize,
    reader: R,
) -> Result<Vec<u8>, SerializationError> {
    ZcashReader::from_stream(reader).read_bytes(external_count)
}

pub(super) fn read_bytes<R: io::Read>(
    external_count: usize,
    reader: &mut ZcashReader<R>,
) -> Result<Vec<u8>, SerializationError> {
    if external_count > MAX_U8_ALLOCATION {
        return Err(SerializationError::Parse(
            "Byte vector longer than MAX_U8_ALLOCATION",
        ));
    }

    if let Some(remaining) = reader.remaining_bytes() {
        if external_count > remaining {
            return Err(SerializationError::Parse(
                "Byte vector exceeds available input",
            ));
        }
        let mut vec = vec![0; external_count];
        reader.read_exact(&mut vec)?;
        return Ok(vec);
    }

    // Unknown streams grow as input arrives. Cap each growth at the declared
    // count, so Vec's geometric growth cannot overshoot the protocol bound.
    let mut vec = Vec::with_capacity(external_count.min(MAX_INITIAL_ALLOCATION));
    while vec.len() < external_count {
        let chunk_end = vec
            .len()
            .saturating_add(MAX_INITIAL_ALLOCATION)
            .min(external_count);
        let chunk_start = vec.len();
        reserve_bounded(&mut vec, chunk_end - chunk_start, external_count);
        vec.resize(chunk_end, 0);
        reader.read_exact(&mut vec[chunk_start..])?;
    }
    Ok(vec)
}

// Reserve enough for this read without doubling past the validated count.
fn reserve_bounded<T>(vec: &mut Vec<T>, additional: usize, count: usize) {
    let required = vec.len().saturating_add(additional);
    if required > vec.capacity() {
        let capacity = vec.capacity().saturating_mul(2).max(required).min(count);
        vec.reserve_exact(capacity - vec.len());
    }
}

/// `zcash_deserialize_external_count`, specialised for [`String`].
/// The external count is in bytes. (Not UTF-8 characters.)
///
/// This allows us to optimize the inner loop into a small number of `read_exact()`
/// calls, rather than one call per byte.
///
/// This function has a `zcash_` prefix to alert the reader that the
/// serialization in use is consensus-critical serialization, rather than
/// some other kind of serialization.
pub fn zcash_deserialize_string_external_count<R: io::Read>(
    external_byte_count: usize,
    reader: R,
) -> Result<String, SerializationError> {
    let bytes = zcash_deserialize_bytes_external_count(external_byte_count, reader)?;

    String::from_utf8(bytes).map_err(|_| SerializationError::Parse("invalid utf-8"))
}

/// Read a Bitcoin-encoded UTF-8 string.
impl ZcashDeserialize for String {
    fn zcash_deserialize_from<R: io::Read>(
        reader: &mut ZcashReader<R>,
    ) -> Result<Self, SerializationError> {
        let byte_count: CompactSizeMessage = reader.read_value()?;
        String::from_utf8(reader.read_bytes(byte_count.into())?)
            .map_err(|_| SerializationError::Parse("invalid utf-8"))
    }
}

// We don't impl ZcashDeserialize for Ipv4Addr or SocketAddrs,
// because the IPv4 and port formats are different in addr (v1) and addrv2 messages.

/// Read a Bitcoin-encoded IPv6 address.
impl ZcashDeserialize for Ipv6Addr {
    fn zcash_deserialize_from<R: io::Read>(
        reader: &mut ZcashReader<R>,
    ) -> Result<Self, SerializationError> {
        let mut ipv6_addr = [0u8; 16];
        reader.read_exact(&mut ipv6_addr)?;

        Ok(Ipv6Addr::from(ipv6_addr))
    }
}

/// Helper for deserializing more succinctly via type inference
pub trait ZcashDeserializeInto {
    /// Deserialize based on type inference
    fn zcash_deserialize_into<T>(self) -> Result<T, SerializationError>
    where
        T: ZcashDeserialize;
}

impl<R: io::Read> ZcashDeserializeInto for R {
    fn zcash_deserialize_into<T>(self) -> Result<T, SerializationError>
    where
        T: ZcashDeserialize,
    {
        T::zcash_deserialize(self)
    }
}

/// Blind preallocation of a `Vec<T: TrustedPreallocate>` is based on a bounded length. This is in contrast
/// to blind preallocation of a generic `Vec<T>`, which is a DOS vector.
///
/// The max_allocation() function provides a loose upper bound on the size of the `Vec<T: TrustedPreallocate>`
/// which can possibly be received from an honest peer. If this limit is too low, Zebra may reject valid messages.
/// In the worst case, setting the lower bound too low could cause Zebra to fall out of consensus by rejecting all messages containing a valid block.
pub trait TrustedPreallocate {
    /// Provides a ***loose upper bound*** on the size of the `Vec<T: TrustedPreallocate>`
    /// which can possibly be received from an honest peer.
    fn max_allocation() -> u64;

    /// Lower bound on bytes consumed by one successfully decoded item, excluding
    /// fields stored in separate arrays. Zero means no bounded decoder contract
    /// has been supplied, and counted decoding from a slice rejects the type.
    fn min_serialized_size() -> u64 {
        0
    }
}

impl<T> TrustedPreallocate for Arc<T>
where
    T: TrustedPreallocate,
{
    fn max_allocation() -> u64 {
        T::max_allocation()
    }

    fn min_serialized_size() -> u64 {
        T::min_serialized_size()
    }
}

/// The length of the longest valid `Vec<u8>` that can be received over the network
///
/// It takes 5 bytes to encode a CompactSize representing any number netween 2^16 and (2^32 - 1)
/// MAX_PROTOCOL_MESSAGE_LEN is ~2^21, so the largest `Vec<u8>` that can be received from an honest peer is
/// (MAX_PROTOCOL_MESSAGE_LEN - 5);
pub(crate) const MAX_U8_ALLOCATION: usize = MAX_PROTOCOL_MESSAGE_LEN - 5;
