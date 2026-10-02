//! A payload reader that knows how many bytes remain.

use super::{Wire, WireError};

/// Reads items from one frame payload.
///
/// Every read is bounded by the bytes that remain, so a decoder can check a
/// list count against the payload before it allocates the list.
#[derive(Debug)]
pub struct BoundedReader<'a> {
    unread: &'a [u8],
}

impl<'a> BoundedReader<'a> {
    /// Start reading `payload` from its first byte.
    pub fn new(payload: &'a [u8]) -> Self {
        Self { unread: payload }
    }

    /// Bytes not yet read.
    pub fn remaining(&self) -> usize {
        self.unread.len()
    }

    /// Consume the remaining payload as opaque bytes without allocating.
    pub(crate) fn take_remaining(&mut self) -> &'a [u8] {
        std::mem::take(&mut self.unread)
    }

    /// Read one typed item.
    pub fn read<T: Wire>(&mut self) -> Result<T::Value, WireError> {
        T::decode(self)
    }

    /// Read the next `N` bytes as an array.
    ///
    /// `item` names the item being read in a truncation error.
    pub fn array<const N: usize>(&mut self, item: &'static str) -> Result<[u8; N], WireError> {
        let Some((bytes, rest)) = self.unread.split_first_chunk::<N>() else {
            return Err(WireError::Truncated(item));
        };
        self.unread = rest;
        Ok(*bytes)
    }

    /// Read a canonical CompactSize count.
    ///
    /// This matches Zcash's CompactSize encoding, and rejects a value that a
    /// shorter encoding could hold. It allocates nothing, even on failure.
    pub fn compact_size(&mut self) -> Result<u64, WireError> {
        const ITEM: &str = "list count";
        let [marker] = self.array::<1>(ITEM)?;
        let (count, smallest) = match marker {
            0xfd => (u64::from(u16::from_le_bytes(self.array(ITEM)?)), 0xfd),
            0xfe => (u64::from(u32::from_le_bytes(self.array(ITEM)?)), 0x1_0000),
            0xff => (u64::from_le_bytes(self.array(ITEM)?), 0x1_0000_0000),
            count => return Ok(u64::from(count)),
        };
        if count < smallest {
            return Err(WireError::NonCanonicalCount);
        }
        Ok(count)
    }

    /// Finish reading and reject trailing bytes.
    pub fn finish(self) -> Result<(), WireError> {
        if !self.unread.is_empty() {
            return Err(WireError::TrailingBytes);
        }
        Ok(())
    }
}

/// Append the canonical CompactSize encoding of `count`.
pub(super) fn write_compact_size(count: u64, out: &mut Vec<u8>) {
    // Each arm's range fits the width it casts to.
    match count {
        0..=0xfc => out.push(count as u8),
        0xfd..=0xffff => {
            out.push(0xfd);
            out.extend_from_slice(&(count as u16).to_le_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            out.push(0xfe);
            out.extend_from_slice(&(count as u32).to_le_bytes());
        }
        _ => {
            out.push(0xff);
            out.extend_from_slice(&count.to_le_bytes());
        }
    }
}

/// Length of the canonical CompactSize encoding of `count`.
pub(super) const fn compact_size_len(count: usize) -> usize {
    match count {
        0..=0xfc => 1,
        0xfd..=0xffff => 3,
        0x1_0000..=0xffff_ffff => 5,
        _ => 9,
    }
}
