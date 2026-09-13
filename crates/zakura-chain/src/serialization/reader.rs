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

use std::io::{self, Read};

use super::{SerializationError, TrustedPreallocate, ZcashDeserialize};

/// Input for a decoder, with the number of bytes still available when known.
///
/// [`Self::from_slice`] starts with bytes already in memory. Reading nested
/// values or applying a smaller read limit preserves the remaining byte count,
/// so collection decoders can check their sizes before allocating memory.
#[derive(Debug)]
pub struct ZcashReader<R> {
    inner: R,
    remaining: Option<usize>,
}

impl<'a, 'b> ZcashReader<&'a mut &'b [u8]> {
    /// Read from bytes already in memory and track how many remain.
    /// The supplied slice advances past each byte consumed.
    pub fn from_slice(bytes: &'a mut &'b [u8]) -> Self {
        let remaining = Some(bytes.len());
        Self {
            inner: bytes,
            remaining,
        }
    }
}

impl<R: Read> ZcashReader<R> {
    pub(super) fn from_stream(inner: R) -> Self {
        Self {
            inner,
            remaining: None,
        }
    }

    /// Number of bytes still available, or `None` if the input length is unknown.
    /// A maximum read limit does not make an unknown stream length known.
    pub fn remaining_bytes(&self) -> Option<usize> {
        self.remaining
    }

    /// Decode one value, letting its decoder see how many bytes remain.
    pub fn read_value<T: ZcashDeserialize>(&mut self) -> Result<T, SerializationError> {
        if self.remaining.is_some() {
            T::zcash_deserialize_from(self)
        } else {
            T::zcash_deserialize(&mut self.inner)
        }
    }

    /// Give a nested decoder permission to read at most `limit` bytes.
    /// Bytes consumed by that decoder also advance this reader.
    ///
    /// If 40 bytes remain and the limit is 100, only 40 bytes are available.
    /// If the stream length is unknown, the limit does not prove any bytes exist.
    pub fn with_limit(&mut self, limit: u64) -> ZcashReader<io::Take<&mut Self>> {
        let remaining = self
            .remaining
            .map(|remaining| remaining.min(usize::try_from(limit).unwrap_or(usize::MAX)));
        ZcashReader {
            inner: Read::take(self, limit),
            remaining,
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
        let length = self
            .remaining
            .map_or(buf.len(), |remaining| remaining.min(buf.len()));
        let read = self.inner.read(&mut buf[..length])?;
        if let Some(remaining) = &mut self.remaining {
            *remaining = remaining.saturating_sub(read);
        }
        Ok(read)
    }
}
