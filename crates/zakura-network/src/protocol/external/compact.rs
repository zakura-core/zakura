//! Compact block relay messages from the draft Compact Block Relay ZIP ([zcash/zips#1429]).
//!
//! A `cmpctblock` message carries a block header, the coinbase transaction, and a six-byte
//! short ID for each other transaction. The receiver rebuilds the block from transactions it
//! already has, and asks for the rest with `getblocktxn`, which the sender answers with
//! `blocktxn`. Peers send these messages only after both set `NODE_COMPACT_BLOCKS`.
//!
//! This module defines the messages, their encodings, the ZIP's limits, and short IDs. Short
//! IDs commit to a transaction's wtxid for v5 and later transactions, so a match can still be
//! the wrong transaction: a receiver must check a rebuilt block's commitments before using it.
//!
//! [zcash/zips#1429]: https://github.com/zcash/zips/pull/1429

use std::{hash::Hasher, io, sync::Arc};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use siphasher::sip::SipHasher24;

use zakura_chain::{
    block::{self, BlockRef, MAX_BLOCK_BYTES},
    serialization::{
        CompactSizeMessage, SerializationError, TrustedPreallocate, ZcashDeserialize, ZcashReader,
        ZcashSerialize,
    },
    transaction::{Transaction, UnminedTxId, MIN_TRANSPARENT_TX_SIZE},
};

#[cfg(test)]
mod tests;

/// The short ID of a transaction in a `cmpctblock` message.
pub type ShortId = [u8; 6];

/// The most transactions a `cmpctblock`, `getblocktxn`, or `blocktxn` message can refer to.
///
/// The ZIP derives this limit from the 2,000,000-byte block size limit divided by 54 bytes, the
/// smallest size of a transaction in a block.
pub const MAX_COMPACT_TRANSACTIONS: usize = 37_037;

// Widening usize to u64 is lossless on supported targets.
const _: () = assert!(MAX_COMPACT_TRANSACTIONS as u64 == MAX_BLOCK_BYTES / MIN_TRANSPARENT_TX_SIZE);

/// The block size limit, in bytes.
// 2,000,000 fits in the usize of every supported target.
const MAX_BLOCK_BYTES_USIZE: usize = MAX_BLOCK_BYTES as usize;

/// The bytes the ZIP's size bound counts for each short ID: the smallest size of the
/// transaction it stands for.
// 54 fits in usize.
const SHORT_ID_TRANSACTION_BYTES: usize = MIN_TRANSPARENT_TX_SIZE as usize;

/// Returns the size of the smallest transaction encoding that the decoder accepts.
fn min_transaction_bytes() -> usize {
    usize::try_from(Transaction::min_serialized_size())
        .expect("the smallest transaction encoding is a few bytes, which fits in usize")
}

/// A block announcement with short IDs in place of the transactions the receiver likely has.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactBlock {
    /// The block header, including its Equihash solution.
    pub header: Arc<block::Header>,

    /// The nonce that keys this message's short IDs, together with the header.
    pub nonce: u64,

    /// The short IDs of the transactions that are not prefilled, in block order.
    pub short_ids: Vec<ShortId>,

    /// The prefilled transactions, in block order. The first is the coinbase transaction.
    pub prefilled: Vec<PrefilledTransaction>,
}

/// A transaction sent in full in a [`CompactBlock`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrefilledTransaction {
    /// The transaction's absolute index in the block.
    ///
    /// The wire format encodes this index relative to the previous prefilled transaction.
    pub index: usize,

    /// The transaction.
    pub transaction: Arc<Transaction>,
}

/// A `getblocktxn` request for the transactions of an announced block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockTransactionsRequest {
    /// The hash of the block announced by `cmpctblock`.
    pub block_hash: block::Hash,

    /// The absolute indexes of the requested transactions, strictly increasing.
    ///
    /// The wire format encodes each index relative to the previous one.
    pub indexes: Vec<usize>,
}

/// A `blocktxn` response with the transactions a [`BlockTransactionsRequest`] asked for.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockTransactions {
    /// The hash of the block, copied from the request.
    pub block_hash: block::Hash,

    /// The requested transactions, in the order they were requested.
    pub transactions: Vec<Arc<Transaction>>,
}

/// The SipHash-2-4 keys of one [`CompactBlock`]'s short IDs.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ShortIdKeys {
    k0: u64,
    k1: u64,
}

impl ShortIdKeys {
    /// Derives the keys from the SHA-256 hash of the encoded `header` followed by `nonce`.
    fn new(header: &block::Header, nonce: u64) -> Self {
        let mut preimage = header
            .zcash_serialize_to_vec()
            .expect("a header serializes like its block hash does, which never fails");
        preimage.extend_from_slice(&nonce.to_le_bytes());
        let hash = ring::digest::digest(&ring::digest::SHA256, &preimage);
        let (k0, rest) = hash
            .as_ref()
            .split_first_chunk::<8>()
            .expect("a SHA-256 hash has 32 bytes");
        let k1 = rest
            .first_chunk::<8>()
            .expect("a SHA-256 hash has 32 bytes");
        Self {
            k0: u64::from_le_bytes(*k0),
            k1: u64::from_le_bytes(*k1),
        }
    }

    /// Returns the short ID of the transaction `id`.
    ///
    /// The short ID hashes the 32-byte txid of a v1 to v4 transaction, and the 64-byte wtxid of
    /// a v5 or later transaction.
    pub fn short_id(&self, id: UnminedTxId) -> ShortId {
        let mut hasher = SipHasher24::new_with_keys(self.k0, self.k1);
        match id {
            UnminedTxId::Legacy(txid) => hasher.write(&txid.0),
            UnminedTxId::Witnessed(wtxid) => hasher.write(&<[u8; 64]>::from(wtxid)),
        }
        let hash = hasher.finish().to_le_bytes();
        *hash
            .first_chunk::<6>()
            .expect("a SipHash-2-4 hash has 8 bytes")
    }
}

impl CompactBlock {
    /// Returns the compact form of `block`, with only its coinbase transaction prefilled.
    ///
    /// Takes a borrowed block, so a block whose owner tracks its memory lends no shared
    /// handles. The message holds copies of the header and the coinbase transaction.
    ///
    /// Choose `nonce` uniformly at random for each message. Fails if the block has no coinbase
    /// transaction first or exceeds the ZIP's limits.
    pub fn from_block(block: &BlockRef, nonce: u64) -> Result<Self, SerializationError> {
        check_count(block.transactions().len())?;
        check_block_bytes(block.zcash_serialized_size())?;
        let mut transactions = block.transactions();
        let coinbase = transactions.next().ok_or(SerializationError::Parse(
            "compact block has no transactions",
        ))?;

        let header = block.header();
        let keys = ShortIdKeys::new(header, nonce);
        let compact = Self {
            header: Arc::new(*header),
            nonce,
            short_ids: transactions
                .map(|transaction| keys.short_id(transaction.into()))
                .collect(),
            prefilled: vec![PrefilledTransaction {
                index: 0,
                transaction: Arc::new(coinbase.clone()),
            }],
        };
        compact.validate()?;
        Ok(compact)
    }

    /// Checks the ZIP's rules for a `cmpctblock` message.
    ///
    /// The message is invalid if:
    /// - it has more than [`MAX_COMPACT_TRANSACTIONS`] transactions;
    /// - the prefilled indexes are not strictly increasing, or one is not less than
    ///   [`Self::transaction_count`];
    /// - the first prefilled transaction is not a coinbase transaction at index 0, or a later
    ///   one is a coinbase transaction; or
    /// - the header, the `CompactSize` encoding of the transaction count, and the prefilled
    ///   transactions, plus 54 bytes for each short ID, take more than 2,000,000 bytes.
    pub fn validate(&self) -> Result<(), SerializationError> {
        let count = self
            .short_ids
            .len()
            .checked_add(self.prefilled.len())
            .ok_or(SerializationError::Parse(
                "compact block transaction count overflows",
            ))?;
        check_count(count)?;

        if !self
            .prefilled
            .first()
            .is_some_and(|first| first.index == 0 && first.transaction.is_coinbase())
        {
            return Err(SerializationError::Parse(
                "compact block does not prefill a coinbase transaction at index 0",
            ));
        }

        let mut bytes = self.fixed_bytes(count)?;
        let mut next = 0;
        for (position, prefilled) in self.prefilled.iter().enumerate() {
            if prefilled.index < next || prefilled.index >= count {
                return Err(SerializationError::Parse(
                    "compact block prefilled indexes are out of order or out of range",
                ));
            }
            if position > 0 && prefilled.transaction.is_coinbase() {
                return Err(SerializationError::Parse(
                    "compact block prefills a coinbase transaction after index 0",
                ));
            }
            // `index < count <= MAX_COMPACT_TRANSACTIONS`, so this can't overflow.
            next = prefilled.index + 1;
            bytes = add_block_bytes(bytes, prefilled.transaction.zcash_serialized_size())?;
        }

        Ok(())
    }

    /// Returns the hash of the announced block.
    pub fn block_hash(&self) -> block::Hash {
        self.header.hash()
    }

    /// Returns the number of transactions in the announced block: the short IDs plus the
    /// prefilled transactions.
    pub fn transaction_count(&self) -> usize {
        self.short_ids.len().saturating_add(self.prefilled.len())
    }

    /// Returns the keys of this message's short IDs.
    pub fn keys(&self) -> ShortIdKeys {
        ShortIdKeys::new(&self.header, self.nonce)
    }

    /// Returns the bytes the ZIP's size bound counts before the prefilled transactions: the
    /// header, the `CompactSize` encoding of the transaction `count`, and 54 bytes for each
    /// short ID. Fails if they exceed the block size limit.
    ///
    /// `count` must be at most [`MAX_COMPACT_TRANSACTIONS`].
    fn fixed_bytes(&self, count: usize) -> Result<usize, SerializationError> {
        // Both counts are at most `MAX_COMPACT_TRANSACTIONS`, so neither term can overflow.
        let short_id_bytes = self.short_ids.len() * SHORT_ID_TRANSACTION_BYTES;
        let count_bytes = CompactSizeMessage::try_from(count)?.zcash_serialized_size();
        let bytes = add_block_bytes(self.header.zcash_serialized_size(), count_bytes)?;
        add_block_bytes(bytes, short_id_bytes)
    }

    /// Decodes a `cmpctblock` message body from untrusted bytes and checks the ZIP's rules.
    ///
    /// Checks each count before allocating for it. Prefilled transactions are decoded with
    /// [`Transaction::zcash_deserialize_bounded`], from no more bytes than the size bound has
    /// left. Leaves `bytes` pointing to any unread bytes.
    pub fn zcash_deserialize_bounded(bytes: &mut &[u8]) -> Result<Self, SerializationError> {
        let header = Arc::new(block::Header::zcash_deserialize_from_slice(bytes)?);
        let nonce = bytes.read_u64::<LittleEndian>()?;

        let short_id_count = read_count(&mut ZcashReader::from_slice(bytes))?;
        // `short_id_count <= MAX_COMPACT_TRANSACTIONS`, so this can't overflow.
        let short_id_bytes = short_id_count * size_of::<ShortId>();
        let Some((short_id_bytes, rest)) = bytes.split_at_checked(short_id_bytes) else {
            return Err(SerializationError::Parse(
                "compact block short IDs exceed the message",
            ));
        };
        // The slice length is a multiple of the short ID size, so no bytes remain.
        let short_ids = short_id_bytes
            .as_chunks::<{ size_of::<ShortId>() }>()
            .0
            .to_vec();
        *bytes = rest;

        let prefilled_count = read_count(&mut ZcashReader::from_slice(bytes))?;
        // Both counts are at most `MAX_COMPACT_TRANSACTIONS`, so this can't overflow.
        let count = short_id_count + prefilled_count;
        check_count(count)?;
        // Each prefilled transaction needs an index byte and a transaction.
        if prefilled_count > bytes.len() / (1 + min_transaction_bytes()) {
            return Err(SerializationError::Parse(
                "compact block prefilled transactions exceed the message",
            ));
        }

        let mut compact = Self {
            header,
            nonce,
            short_ids,
            prefilled: Vec::with_capacity(prefilled_count),
        };
        let mut block_bytes = compact.fixed_bytes(count)?;
        let mut next = 0;
        for _ in 0..prefilled_count {
            let index = read_index(&mut ZcashReader::from_slice(bytes), &mut next, count)?;
            let transaction = read_transaction(bytes, &mut block_bytes)?;
            compact.prefilled.push(PrefilledTransaction {
                index,
                transaction: Arc::new(transaction),
            });
        }

        compact.validate()?;
        Ok(compact)
    }
}

impl ZcashSerialize for CompactBlock {
    /// Encodes a `cmpctblock` message body. Fails if [`CompactBlock::validate`] fails.
    fn zcash_serialize<W: io::Write + ?Sized>(&self, writer: &mut W) -> Result<(), io::Error> {
        self.validate().map_err(io::Error::other)?;

        self.header.zcash_serialize(writer)?;
        writer.write_u64::<LittleEndian>(self.nonce)?;
        write_count(writer, self.short_ids.len())?;
        for short_id in &self.short_ids {
            writer.write_all(short_id)?;
        }
        write_count(writer, self.prefilled.len())?;
        let mut next = 0;
        for prefilled in &self.prefilled {
            write_index(writer, prefilled.index, &mut next)?;
            prefilled.transaction.zcash_serialize(writer)?;
        }
        Ok(())
    }
}

impl ZcashSerialize for BlockTransactionsRequest {
    /// Encodes a `getblocktxn` message body. Fails if there are more than
    /// [`MAX_COMPACT_TRANSACTIONS`] indexes, or they are not strictly increasing and less than
    /// [`MAX_COMPACT_TRANSACTIONS`].
    fn zcash_serialize<W: io::Write + ?Sized>(&self, writer: &mut W) -> Result<(), io::Error> {
        check_count(self.indexes.len()).map_err(io::Error::other)?;

        self.block_hash.zcash_serialize(writer)?;
        write_count(writer, self.indexes.len())?;
        let mut next = 0;
        for index in &self.indexes {
            if *index >= MAX_COMPACT_TRANSACTIONS {
                return Err(io::Error::other(
                    "compact transaction index exceeds the transaction limit",
                ));
            }
            write_index(writer, *index, &mut next)?;
        }
        Ok(())
    }
}

impl ZcashDeserialize for BlockTransactionsRequest {
    /// Decodes a `getblocktxn` message body.
    ///
    /// The decoder doesn't know the block's transaction count, so it rejects indexes that are
    /// not less than [`MAX_COMPACT_TRANSACTIONS`]. The receiver checks them against the block.
    fn zcash_deserialize_from<R: io::Read>(
        reader: &mut ZcashReader<R>,
    ) -> Result<Self, SerializationError> {
        let block_hash = reader.read_value()?;
        let count = read_count(reader)?;
        // Each index takes at least one byte.
        if reader
            .remaining_bytes()
            .is_some_and(|remaining| count > remaining)
        {
            return Err(SerializationError::Parse(
                "compact transaction indexes exceed the message",
            ));
        }

        let mut indexes = Vec::with_capacity(count);
        let mut next = 0;
        for _ in 0..count {
            indexes.push(read_index(reader, &mut next, MAX_COMPACT_TRANSACTIONS)?);
        }
        Ok(Self {
            block_hash,
            indexes,
        })
    }
}

impl ZcashSerialize for BlockTransactions {
    /// Encodes a `blocktxn` message body. Fails if there are more than
    /// [`MAX_COMPACT_TRANSACTIONS`] transactions, or they take more than 2,000,000 bytes.
    fn zcash_serialize<W: io::Write + ?Sized>(&self, writer: &mut W) -> Result<(), io::Error> {
        check_count(self.transactions.len()).map_err(io::Error::other)?;
        self.transactions
            .iter()
            .try_fold(0, |bytes, transaction| {
                add_block_bytes(bytes, transaction.zcash_serialized_size())
            })
            .map_err(io::Error::other)?;

        self.block_hash.zcash_serialize(writer)?;
        write_count(writer, self.transactions.len())?;
        for transaction in &self.transactions {
            transaction.zcash_serialize(writer)?;
        }
        Ok(())
    }
}

impl BlockTransactions {
    /// Decodes a `blocktxn` message body from untrusted bytes.
    ///
    /// Checks the transaction count before allocating for it. Transactions are decoded with
    /// [`Transaction::zcash_deserialize_bounded`], from no more bytes than the 2,000,000-byte
    /// total has left. Leaves `bytes` pointing to any unread bytes.
    pub fn zcash_deserialize_bounded(bytes: &mut &[u8]) -> Result<Self, SerializationError> {
        let block_hash = block::Hash::zcash_deserialize_from_slice(bytes)?;
        let count = read_count(&mut ZcashReader::from_slice(bytes))?;
        if count > bytes.len() / min_transaction_bytes() {
            return Err(SerializationError::Parse(
                "compact recovery transactions exceed the message",
            ));
        }

        let mut transactions = Vec::with_capacity(count);
        let mut transaction_bytes = 0;
        for _ in 0..count {
            transactions.push(Arc::new(read_transaction(bytes, &mut transaction_bytes)?));
        }
        Ok(Self {
            block_hash,
            transactions,
        })
    }
}

/// Fails if `count` exceeds [`MAX_COMPACT_TRANSACTIONS`].
fn check_count(count: usize) -> Result<(), SerializationError> {
    if count > MAX_COMPACT_TRANSACTIONS {
        return Err(SerializationError::Parse(
            "compact transaction count exceeds the transaction limit",
        ));
    }
    Ok(())
}

/// Fails if `bytes` exceeds the block size limit.
fn check_block_bytes(bytes: usize) -> Result<(), SerializationError> {
    if bytes > MAX_BLOCK_BYTES_USIZE {
        return Err(SerializationError::Parse(
            "compact block relay bytes exceed the block size limit",
        ));
    }
    Ok(())
}

/// Returns `bytes + extra`, failing if the sum exceeds the block size limit.
fn add_block_bytes(bytes: usize, extra: usize) -> Result<usize, SerializationError> {
    let sum = bytes.saturating_add(extra);
    check_block_bytes(sum)?;
    Ok(sum)
}

/// Reads a `CompactSize` transaction count, failing if it exceeds
/// [`MAX_COMPACT_TRANSACTIONS`].
fn read_count<R: io::Read>(reader: &mut ZcashReader<R>) -> Result<usize, SerializationError> {
    let count = usize::from(reader.read_value::<CompactSizeMessage>()?);
    check_count(count)?;
    Ok(count)
}

/// Reads one differentially encoded index and returns the absolute index.
///
/// `next` is the smallest absolute index allowed, one more than the previous index. Fails if
/// the absolute index is not less than `limit`.
fn read_index<R: io::Read>(
    reader: &mut ZcashReader<R>,
    next: &mut usize,
    limit: usize,
) -> Result<usize, SerializationError> {
    let delta = usize::from(reader.read_value::<CompactSizeMessage>()?);
    let index = next
        .checked_add(delta)
        .filter(|index| *index < limit)
        .ok_or(SerializationError::Parse(
            "compact transaction index is out of range",
        ))?;
    // `index < limit`, so this can't overflow.
    *next = index + 1;
    Ok(index)
}

/// Reads a transaction from `bytes`, adding its size to `block_bytes`.
///
/// The transaction is decoded from at most the bytes left under the block size limit, so a
/// transaction that would exceed it fails to decode.
fn read_transaction(
    bytes: &mut &[u8],
    block_bytes: &mut usize,
) -> Result<Transaction, SerializationError> {
    let budget = MAX_BLOCK_BYTES_USIZE.saturating_sub(*block_bytes);
    let available = bytes.len().min(budget);
    let mut limited = &bytes[..available];
    let transaction = Transaction::zcash_deserialize_bounded(&mut limited)?;
    let read = available - limited.len();
    *block_bytes += read;
    *bytes = &bytes[read..];
    Ok(transaction)
}

/// Writes a `CompactSize` transaction count.
fn write_count<W: io::Write + ?Sized>(writer: &mut W, count: usize) -> io::Result<()> {
    CompactSizeMessage::try_from(count)
        .map_err(io::Error::other)?
        .zcash_serialize(writer)
}

/// Writes the absolute `index` differentially encoded.
///
/// `next` is the smallest absolute index allowed, one more than the previous index. Fails if
/// `index` is less than `next`.
fn write_index<W: io::Write + ?Sized>(
    writer: &mut W,
    index: usize,
    next: &mut usize,
) -> io::Result<()> {
    let delta = index
        .checked_sub(*next)
        .ok_or_else(|| io::Error::other("compact transaction indexes are not increasing"))?;
    *next = index
        .checked_add(1)
        .ok_or_else(|| io::Error::other("compact transaction index overflows"))?;
    write_count(writer, delta)
}
