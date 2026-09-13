//! Input bounds shared by nested consensus decoders.

use std::io::{self, Read};

use super::{SerializationError, TrustedPreallocate, ZcashDeserialize};

/// A decoder input that distinguishes bytes present from a stream read allowance.
///
/// Construct this from a slice to carry its actual length through nested values
/// and protocol limits. Streaming callers retain their existing entry points.
#[derive(Debug)]
pub struct ZcashReader<R> {
    inner: R,
    remaining: Option<usize>,
}

impl<'a, 'b> ZcashReader<&'a mut &'b [u8]> {
    /// Read from the supplied bytes, advancing the slice as input is consumed.
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

    /// Bytes actually available within this reader, or `None` for streaming input.
    /// A protocol limit alone never turns an unknown stream length into known data.
    pub fn remaining_bytes(&self) -> Option<usize> {
        self.remaining
    }

    /// Decode a nested value without losing this input's allocation bounds.
    pub fn read_value<T: ZcashDeserialize>(&mut self) -> Result<T, SerializationError> {
        if self.remaining.is_some() {
            T::zcash_deserialize_from(self)
        } else {
            T::zcash_deserialize(&mut self.inner)
        }
    }

    /// Restrict a nested object while preserving the distinction between actual
    /// input and a maximum allowance. The parent advances with the child.
    pub fn with_limit(&mut self, limit: u64) -> ZcashReader<io::Take<&mut Self>> {
        let remaining = self
            .remaining
            .map(|remaining| remaining.min(usize::try_from(limit).unwrap_or(usize::MAX)));
        ZcashReader {
            inner: Read::take(self, limit),
            remaining,
        }
    }

    /// Decode an externally counted collection with the same bounds as `Vec<T>`.
    pub fn read_external_count<T: ZcashDeserialize + TrustedPreallocate>(
        &mut self,
        count: usize,
    ) -> Result<Vec<T>, SerializationError> {
        super::zcash_deserialize::read_external_count(count, self)
    }

    /// Decode an externally counted byte string without trusting its count alone.
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
