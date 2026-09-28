//! A CompactSize-prefixed list item.

use std::marker::PhantomData;

use super::{
    reader::{compact_size_len, write_compact_size},
    BoundedReader, Wire, WireError,
};

/// A CompactSize count followed by `MIN..=MAX` items of type `T`.
///
/// Decoding checks the count against `MIN..=MAX` and against the bytes that
/// remain, so a list never reserves memory for items its payload cannot hold.
/// It then reserves space for exactly `count` items. `T` must have a nonzero
/// minimum length; otherwise the remaining bytes would not bound the count.
///
/// ```
/// # use zakura_network::zakura::wire_codec::{List, Wire, U8};
/// // One to 1024 bytes, after a one- or three-byte count.
/// type ItemBytes = List<U8, 1, 1024>;
/// assert_eq!((ItemBytes::MIN_LEN, ItemBytes::MAX_LEN), (2, 1027));
/// ```
#[derive(Debug)]
pub struct List<T, const MIN: usize, const MAX: usize>(PhantomData<fn() -> T>);

impl<T: Wire, const MIN: usize, const MAX: usize> List<T, MIN, MAX> {
    /// Fail the build for a list whose bounds cannot hold.
    const VALID: () = {
        assert!(MIN <= MAX, "a list minimum must not exceed its maximum");
        assert!(T::MIN_LEN > 0, "list items need a nonzero minimum length");
    };

    /// Most items that `input_len` bytes can hold.
    fn max_count(input_len: usize) -> usize {
        MAX.min(input_len / T::MIN_LEN)
    }
}

impl<T: Wire, const MIN: usize, const MAX: usize> Wire for List<T, MIN, MAX> {
    type Value = Vec<T::Value>;
    const MIN_LEN: usize = {
        let () = Self::VALID;
        compact_size_len(MIN) + MIN * T::MIN_LEN
    };
    const MAX_LEN: usize = compact_size_len(MAX) + MAX * T::MAX_LEN;

    fn max_heap_bytes(input_len: usize) -> usize {
        // The list's buffer, plus each item's own heap. Each item reads at most
        // `input_len` bytes.
        Self::max_count(input_len)
            .saturating_mul(size_of::<T::Value>().saturating_add(T::max_heap_bytes(input_len)))
    }

    fn encode(values: &Vec<T::Value>, out: &mut Vec<u8>) -> Result<(), WireError> {
        let () = Self::VALID;
        let count = values.len();
        if !(MIN..=MAX).contains(&count) {
            return Err(WireError::CountOutOfRange {
                count: u64::try_from(count).unwrap_or(u64::MAX),
                min: MIN,
                max: MAX,
            });
        }
        write_compact_size(u64::try_from(count).unwrap_or(u64::MAX), out);
        for value in values {
            T::encode(value, out)?;
        }
        Ok(())
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<Vec<T::Value>, WireError> {
        let () = Self::VALID;
        let declared = reader.compact_size()?;
        let count = usize::try_from(declared)
            .ok()
            .filter(|count| (MIN..=MAX).contains(count))
            .ok_or(WireError::CountOutOfRange {
                count: declared,
                min: MIN,
                max: MAX,
            })?;
        if count > Self::max_count(reader.remaining()) {
            return Err(WireError::CountExceedsPayload {
                count: declared,
                remaining: reader.remaining(),
            });
        }
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(T::decode(reader)?);
        }
        Ok(values)
    }
}
