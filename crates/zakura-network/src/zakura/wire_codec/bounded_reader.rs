//! A payload reader that knows how many bytes remain.

use std::io::Cursor;

use zakura_chain::serialization::{CompactSize64, ZcashDeserialize};

use super::{Wire, WireError};

/// Reads fields from one frame payload.
///
/// Every read is bounded by the bytes that remain, so a list count can be
/// checked against the payload before the list is allocated.
#[derive(Debug)]
pub struct BoundedReader<'a> {
    payload: &'a [u8],
    position: usize,
}

impl<'a> BoundedReader<'a> {
    /// Start reading `payload` from its first byte.
    pub fn new(payload: &'a [u8]) -> Self {
        Self {
            payload,
            position: 0,
        }
    }

    /// Bytes not yet read.
    pub fn remaining(&self) -> usize {
        self.payload.len() - self.position
    }

    /// Read the next `len` bytes.
    pub fn take(&mut self, len: usize, field: &'static str) -> Result<&'a [u8], WireError> {
        if len > self.remaining() {
            return Err(WireError::Truncated(field));
        }
        let bytes = &self.payload[self.position..self.position + len];
        self.position += len;
        Ok(bytes)
    }

    /// Read the next `N` bytes as an array.
    pub fn array<const N: usize>(&mut self, field: &'static str) -> Result<[u8; N], WireError> {
        let mut bytes = [0; N];
        bytes.copy_from_slice(self.take(N, field)?);
        Ok(bytes)
    }

    /// Read one typed wire item.
    pub fn read<T: Wire>(&mut self) -> Result<T::Value, WireError> {
        T::decode(self)
    }

    /// Read one Zcash-serialized value from the remaining bytes.
    pub fn zcash<T: ZcashDeserialize>(&mut self) -> Result<T, WireError> {
        let mut cursor = Cursor::new(&self.payload[self.position..]);
        let value = T::zcash_deserialize(&mut cursor)?;
        // The cursor reads a slice of `remaining()` bytes, so its position fits usize.
        self.position += cursor.position() as usize;
        Ok(value)
    }

    /// Read a canonical CompactSize count.
    pub fn compact_size(&mut self) -> Result<u64, WireError> {
        Ok(u64::from(self.zcash::<CompactSize64>()?))
    }

    /// Check a declared list count before the list is allocated.
    ///
    /// The count must be within `min..=max`, and the remaining payload must be
    /// able to hold `count` items of at least `min_item_len` bytes each.
    pub fn check_count(
        &self,
        list: &'static str,
        count: u64,
        min: usize,
        max: usize,
        min_item_len: usize,
    ) -> Result<usize, WireError> {
        let declared = count;
        let out_of_range = WireError::CountOutOfRange {
            list,
            count: declared,
            min,
            max,
        };
        let count = usize::try_from(count).map_err(|_| out_of_range.clone())?;
        if count < min || count > max {
            return Err(out_of_range);
        }
        let needed = count.checked_mul(min_item_len);
        if needed.is_none_or(|needed| needed > self.remaining()) {
            return Err(WireError::CountExceedsPayload {
                list,
                count: declared,
                remaining: self.remaining(),
            });
        }
        Ok(count)
    }

    /// Finish reading and reject any trailing bytes.
    pub fn finish(self) -> Result<(), WireError> {
        if self.remaining() != 0 {
            return Err(WireError::TrailingBytes);
        }
        Ok(())
    }
}
