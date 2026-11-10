//! The consensus encoding a block was decoded from, kept so it is not rebuilt.
//!
//! A node receives every block as bytes, decodes it, and then needs those
//! bytes again: to hash pre-v5 transactions, to store each transaction, to
//! measure the block, and to send it to peers and RPC clients. Re-serializing
//! the decoded block rebuilds the same bytes each time. [`BlockEncoding`] keeps
//! the received bytes instead, and can only be built from a block that
//! re-encodes to exactly those bytes.

use std::{fmt, io, ops::Range, sync::Arc};

use crate::{
    serialization::{
        sha256d, CompactSizeMessage, SerializationError, ZcashDeserialize, ZcashSerialize,
    },
    transaction::{self, Transaction},
};

use super::{Block, PARALLEL_TRANSACTION_HASHES_MIN};

/// The consensus encoding of a decoded block, and where each transaction is in it.
///
/// It holds exactly the bytes that serializing its block produces: the only
/// constructors decode the block from the bytes, or check that the block
/// re-encodes to them, byte for byte.
#[derive(Clone)]
pub struct BlockEncoding {
    /// The buffer the block was decoded from. It can hold bytes before the block.
    buffer: Arc<[u8]>,
    /// Where the block is in `buffer`.
    block: Range<usize>,
    /// Where each transaction ends, relative to the start of the block, in block order.
    transaction_ends: Box<[usize]>,
    /// Where the first transaction starts, relative to the start of the block.
    transactions_start: usize,
}

impl fmt::Debug for BlockEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockEncoding")
            .field("len", &self.len())
            .field("transactions", &self.transaction_ends.len())
            .finish()
    }
}

impl BlockEncoding {
    /// Decodes the block that fills `buffer[start..]`, keeping `buffer` as its encoding.
    ///
    /// Returns an error if the bytes do not decode to exactly one block. Returns no
    /// encoding if the block does not re-encode to the same bytes, which the
    /// canonical-encoding checks in the decoder rule out for valid input.
    pub fn decode(
        buffer: Arc<[u8]>,
        start: usize,
    ) -> Result<(Block, Option<Self>), SerializationError> {
        let mut bytes = buffer.get(start..).ok_or(SerializationError::Parse(
            "block start is past the end of the buffer",
        ))?;
        let block = Block::zcash_deserialize_from_slice(&mut bytes)?;
        if !bytes.is_empty() {
            return Err(SerializationError::Parse("trailing bytes after a block"));
        }
        let end = buffer.len();
        let encoding = Self::for_block(&block, buffer, start..end);
        Ok((block, encoding))
    }

    /// Returns `buffer[block_range]` as the encoding of `block`, if serializing
    /// `block` produces exactly those bytes.
    ///
    /// The check serializes `block` into a comparing writer, without allocating.
    pub fn for_block(block: &Block, buffer: Arc<[u8]>, block_range: Range<usize>) -> Option<Self> {
        let bytes = buffer.get(block_range.clone())?;

        let mut writer = ComparingWriter {
            expected: bytes,
            position: 0,
            matches: true,
        };
        block.header.zcash_serialize(&mut writer).ok()?;
        CompactSizeMessage::try_from(block.transactions.len())
            .ok()?
            .zcash_serialize(&mut writer)
            .ok()?;
        let transactions_start = writer.position;
        let mut transaction_ends = Vec::with_capacity(block.transactions.len());
        for transaction in &block.transactions {
            transaction.zcash_serialize(&mut writer).ok()?;
            transaction_ends.push(writer.position);
        }
        if !writer.matches || writer.position != bytes.len() {
            return None;
        }

        Some(Self {
            buffer,
            block: block_range,
            transaction_ends: transaction_ends.into_boxed_slice(),
            transactions_start,
        })
    }

    /// The block's consensus encoding.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer[self.block.clone()]
    }

    /// The length of the block's encoding, which is the block's serialized size.
    pub fn len(&self) -> usize {
        self.block.len()
    }

    /// Whether the encoding is empty, which a decoded block never is.
    pub fn is_empty(&self) -> bool {
        self.block.is_empty()
    }

    /// The number of transactions in the block.
    pub fn transaction_count(&self) -> usize {
        self.transaction_ends.len()
    }

    /// The encoding of the transaction at `index`, if the block has one.
    pub fn transaction_bytes(&self, index: usize) -> Option<&[u8]> {
        let end = *self.transaction_ends.get(index)?;
        let start = match index.checked_sub(1) {
            Some(previous) => self.transaction_ends[previous],
            None => self.transactions_start,
        };
        Some(&self.as_bytes()[start..end])
    }

    /// The encoding of each transaction, in block order.
    pub fn transactions_bytes(&self) -> impl ExactSizeIterator<Item = &[u8]> + '_ {
        (0..self.transaction_count()).map(|index| {
            self.transaction_bytes(index)
                .expect("the index is below the transaction count")
        })
    }

    /// Every transaction's mined ID, in block order, for `block`, the block this
    /// encodes.
    ///
    /// A pre-v5 transaction ID is the SHA-256d hash of the transaction's encoding,
    /// so it is hashed from the kept bytes instead of serializing the transaction
    /// again. Later versions use their ZIP-244 digests.
    ///
    /// # Panics
    ///
    /// If `block` has a different number of transactions than this encoding.
    pub fn transaction_hashes(&self, block: &Block) -> Arc<[transaction::Hash]> {
        assert_eq!(
            block.transactions.len(),
            self.transaction_count(),
            "the encoding belongs to this block"
        );

        let hash = |(transaction, bytes): (&Arc<Transaction>, &[u8])| {
            if transaction.version() <= 4 {
                let mut writer = sha256d::Writer::default();
                io::Write::write_all(&mut writer, bytes).expect("hash writers accept every byte");
                transaction::Hash(writer.finish())
            } else {
                transaction.hash()
            }
        };

        if block.transactions.len() < PARALLEL_TRANSACTION_HASHES_MIN
            || rayon::current_num_threads() == 1
        {
            block
                .transactions
                .iter()
                .zip(self.transactions_bytes())
                .map(hash)
                .collect()
        } else {
            use rayon::prelude::*;
            block
                .transactions
                .par_iter()
                .zip((0..self.transaction_count()).into_par_iter().map(|index| {
                    self.transaction_bytes(index)
                        .expect("the index is below the transaction count")
                }))
                .map(hash)
                .collect::<Vec<_>>()
                .into()
        }
    }
}

/// A writer that checks the bytes written to it against `expected`.
struct ComparingWriter<'a> {
    expected: &'a [u8],
    position: usize,
    matches: bool,
}

impl io::Write for ComparingWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let end = self.position.saturating_add(buf.len());
        if self.expected.get(self.position..end) != Some(buf) {
            self.matches = false;
        }
        self.position = end;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
