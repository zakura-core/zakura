//! Keep track of the bytes left while decoding a complete message.
//!
//! A message can claim to contain more items than its bytes can hold. For example,
//! two 32-byte hashes need 64 bytes. If only 40 bytes remain, the collection
//! decoder can reject that count before reserving memory for the hashes.
//!
//! [`ZcashReader`] carries the remaining byte count into each nested decoder, so
//! the same check works for collections inside blocks, transactions, and proofs.
//! A stream whose length is unknown cannot provide this check. A maximum read
//! limit alone does not tell us how many bytes are actually present.

use std::{
    io::{self, Read},
    ops::{Deref, DerefMut},
};

use super::{SerializationError, TrustedPreallocate, ZcashDeserialize};

/// Input for a decoder, with the number of bytes still available when known.
///
/// [`Self::from_slice`] starts with bytes already in memory. Reading nested
/// values or applying a smaller read limit preserves the remaining byte count.
#[derive(Debug)]
pub struct ZcashReader<R> {
    inner: R,
    remaining: Option<usize>,
    /// Bytes the innermost [`Self::with_limit`] still allows, or `None` without a limit.
    limit: Option<u64>,
}

impl<'a, 'b> ZcashReader<&'a mut &'b [u8]> {
    /// Read from bytes already in memory and track how many remain.
    /// The supplied slice advances past each byte consumed.
    pub fn from_slice(bytes: &'a mut &'b [u8]) -> Self {
        let remaining = Some(bytes.len());
        Self {
            inner: bytes,
            remaining,
            limit: None,
        }
    }
}

impl<R: Read> ZcashReader<R> {
    pub(super) fn from_stream(inner: R) -> Self {
        Self {
            inner,
            remaining: None,
            limit: None,
        }
    }

    /// Number of bytes still available, or `None` if the input length is unknown.
    /// A maximum read limit does not make an unknown stream length known.
    pub fn remaining_bytes(&self) -> Option<usize> {
        self.remaining.map(|remaining| match self.limit {
            Some(limit) => remaining.min(usize::try_from(limit).unwrap_or(usize::MAX)),
            None => remaining,
        })
    }

    /// Decode one value, letting its decoder see how many bytes remain.
    pub fn read_value<T: ZcashDeserialize>(&mut self) -> Result<T, SerializationError> {
        if self.remaining.is_some() {
            T::zcash_deserialize_from(self)
        } else {
            // Read through `self`, not `self.inner`, so an active limit still applies.
            T::zcash_deserialize(self)
        }
    }

    /// Give a nested decoder permission to read at most `limit` bytes.
    /// Bytes consumed by that decoder also advance this reader.
    ///
    /// If 40 bytes remain and the limit is 100, only 40 bytes are available.
    /// If the stream length is unknown, the limit does not prove any bytes exist.
    ///
    /// The limit lasts until the returned guard is dropped. The guard dereferences
    /// to this same reader, so nested decoders are not instantiated for a new type.
    pub fn with_limit(&mut self, limit: u64) -> LimitedReader<'_, R> {
        let outer_limit = self.limit;
        let entry_limit = outer_limit.map_or(limit, |outer| outer.min(limit));
        self.limit = Some(entry_limit);
        LimitedReader {
            reader: self,
            outer_limit,
            entry_limit,
        }
    }

    /// Read `count` items when their count was decoded earlier or supplied by a rule.
    /// Reject counts that exceed protocol limits or cannot fit in the known input
    /// before allocating the collection.
    pub fn read_external_count<T: ZcashDeserialize + TrustedPreallocate>(
        &mut self,
        count: usize,
    ) -> Result<Vec<T>, SerializationError> {
        super::zcash_deserialize::read_external_count(count, self)
    }

    /// Read `count` bytes into a new buffer. Reject counts that exceed protocol
    /// limits or the known remaining bytes before allocating it.
    pub fn read_bytes(&mut self, count: usize) -> Result<Vec<u8>, SerializationError> {
        super::zcash_deserialize::read_bytes(count, self)
    }
}

impl<R: Read> Read for ZcashReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Like `io::Take`, an exhausted limit reports end of input without
        // touching the inner reader.
        if self.limit == Some(0) {
            return Ok(0);
        }
        let mut length = self
            .remaining
            .map_or(buf.len(), |remaining| remaining.min(buf.len()));
        if let Some(limit) = self.limit {
            length = length.min(usize::try_from(limit).unwrap_or(usize::MAX));
        }
        let read = self.inner.read(&mut buf[..length])?;
        if let Some(remaining) = &mut self.remaining {
            *remaining = remaining.saturating_sub(read);
        }
        if let Some(limit) = &mut self.limit {
            *limit = limit.saturating_sub(u64::try_from(read).unwrap_or(u64::MAX));
        }
        Ok(read)
    }
}

/// A [`ZcashReader`] under a temporary read limit, from [`ZcashReader::with_limit`].
///
/// Dropping it restores the enclosing limit, reduced by the bytes read under this one.
#[derive(Debug)]
pub struct LimitedReader<'a, R> {
    reader: &'a mut ZcashReader<R>,
    outer_limit: Option<u64>,
    entry_limit: u64,
}

impl<R> Deref for LimitedReader<'_, R> {
    type Target = ZcashReader<R>;

    fn deref(&self) -> &Self::Target {
        self.reader
    }
}

impl<R> DerefMut for LimitedReader<'_, R> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.reader
    }
}

impl<R> Drop for LimitedReader<'_, R> {
    fn drop(&mut self) {
        // Reads only lower the limit, so the current limit never exceeds `entry_limit`.
        let consumed = self
            .entry_limit
            .saturating_sub(self.reader.limit.unwrap_or(self.entry_limit));
        self.reader.limit = self.outer_limit.map(|outer| outer.saturating_sub(consumed));
    }
}
