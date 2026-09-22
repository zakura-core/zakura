//! Const descriptors for count-prefixed lists.

use std::marker::PhantomData;

use zakura_chain::serialization::{CompactSize64, ZcashSerialize};

use super::{BoundedReader, Wire, WireError};

/// A CompactSize-prefixed list of `T` items with `min_count..=max_count` items.
///
/// A descriptor is a `const`, not a container: messages keep plain `Vec`
/// fields and name the descriptor that encodes and decodes them. Decoding
/// checks the count against both the descriptor and the remaining payload
/// before it allocates the list.
#[derive(Debug)]
pub struct BoundedVec<T> {
    /// List name used in errors.
    pub list: &'static str,
    /// Fewest items a valid list holds.
    pub min_count: usize,
    /// Most items a valid list holds.
    pub max_count: usize,
    item: PhantomData<fn() -> T>,
}

impl<T: Wire> BoundedVec<T> {
    /// Describe a list of `min_count..=max_count` items.
    pub const fn new(list: &'static str, min_count: usize, max_count: usize) -> Self {
        assert!(
            min_count <= max_count,
            "list minimum must not exceed its maximum"
        );
        Self {
            list,
            min_count,
            max_count,
            item: PhantomData,
        }
    }

    /// Smallest valid encoding in bytes.
    pub const fn min_len(&self) -> usize {
        compact_size_len(self.min_count).saturating_add(self.min_count.saturating_mul(T::MIN_LEN))
    }

    /// Largest valid encoding in bytes.
    pub const fn max_len(&self) -> usize {
        compact_size_len(self.max_count).saturating_add(self.max_count.saturating_mul(T::MAX_LEN))
    }

    /// Most heap bytes that [`Self::decode`] reserves for its list from a
    /// payload of `payload_len` bytes.
    ///
    /// Item values may own further heap data; this bound covers only the
    /// list's own buffer.
    #[cfg(test)]
    pub fn allocation_bound(&self, payload_len: usize) -> usize {
        let fitting_items = payload_len / T::MIN_LEN.max(1);
        fitting_items
            .min(self.max_count)
            .saturating_mul(size_of::<T::Value>())
    }

    /// Check that a list of `len` items fits this descriptor.
    pub fn check_len(&self, len: usize) -> Result<(), WireError> {
        if len < self.min_count || len > self.max_count {
            return Err(WireError::CountOutOfRange {
                list: self.list,
                count: u64::try_from(len).unwrap_or(u64::MAX),
                min: self.min_count,
                max: self.max_count,
            });
        }
        Ok(())
    }

    /// Append the count prefix and every item.
    pub fn encode(&self, items: &[T::Value], out: &mut Vec<u8>) -> Result<(), WireError> {
        self.check_len(items.len())?;
        let count = u64::try_from(items.len()).map_err(|_| WireError::OutOfRange(self.list))?;
        CompactSize64::from(count).zcash_serialize(&mut *out)?;
        for item in items {
            T::encode(item, out)?;
        }
        Ok(())
    }

    /// Read the count prefix, check it, then read every item.
    pub fn decode(&self, reader: &mut BoundedReader<'_>) -> Result<Vec<T::Value>, WireError> {
        let declared = reader.compact_size()?;
        let count = reader.check_count(
            self.list,
            declared,
            self.min_count,
            self.max_count,
            T::MIN_LEN,
        )?;
        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            items.push(T::decode(reader)?);
        }
        Ok(items)
    }
}

/// Encoded length of a canonical CompactSize for `count`.
pub const fn compact_size_len(count: usize) -> usize {
    match count {
        0..=0xfc => 1,
        0xfd..=0xffff => 3,
        0x1_0000..=0xffff_ffff => 5,
        _ => 9,
    }
}
