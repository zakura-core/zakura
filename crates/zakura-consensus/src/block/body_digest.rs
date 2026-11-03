//! A digest of a block's serialized transactions, shared by the block identities the verifier
//! derives.

use std::io::{self, Write};

use blake2b_simd::{Params, State};
use zakura_chain::{
    block::{Block, Header},
    serialization::ZcashSerialize,
};

/// Blake2b-256 of a block's serialized transaction list, including its count.
///
/// The verifier identifies a submitted block twice, by its complete content for receipt
/// ordering and by its solution-independent content for prepared candidates. Both identities
/// cover the same transactions, so the block body is serialized and hashed once and each
/// identity hashes its own header bytes with this digest.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct BodyDigest([u8; 32]);

impl BodyDigest {
    /// Hashes `block`'s transactions.
    pub(super) fn of(block: &Block) -> Self {
        let mut writer = BatchedHashWriter::new();
        block
            .transactions
            .zcash_serialize(&mut writer)
            .expect("serializing parsed transactions to a hash cannot fail");
        Self(writer.finalize())
    }

    /// Blake2b-256 of `header`'s serialization followed by this digest.
    pub(super) fn with_header(&self, header: &Header) -> [u8; 32] {
        let mut writer = BatchedHashWriter::new();
        header
            .zcash_serialize(&mut writer)
            .expect("serializing a header to a hash cannot fail");
        writer
            .write_all(&self.0)
            .expect("writing to a hash cannot fail");
        writer.finalize()
    }
}

/// The number of serialized bytes [`BatchedHashWriter`] gathers before hashing them.
///
/// Larger batches measured no faster on blocks up to the 2 MB limit.
const BATCH_BYTES: usize = 4096;

/// Feeds serialized bytes to Blake2b-256 in batches of up to [`BATCH_BYTES`].
///
/// Serializing a block issues one write per field, most of them a few bytes long, and every
/// [`State::update`] call has a fixed cost that is significant at that size. Gathering the
/// writes in a fixed array keeps that cost per batch without buffering the whole block. The
/// digest is the same as hashing the serialized bytes in one call.
struct BatchedHashWriter {
    state: State,
    batch: [u8; BATCH_BYTES],
    len: usize,
}

impl BatchedHashWriter {
    fn new() -> Self {
        Self {
            state: Params::new().hash_length(32).to_state(),
            batch: [0; BATCH_BYTES],
            len: 0,
        }
    }

    /// Hashes the pending batch.
    fn hash_batch(&mut self) {
        self.state.update(&self.batch[..self.len]);
        self.len = 0;
    }

    /// Returns the Blake2b-256 digest of every byte written.
    fn finalize(mut self) -> [u8; 32] {
        self.hash_batch();
        let mut digest = [0; 32];
        digest.copy_from_slice(self.state.finalize().as_bytes());
        digest
    }
}

impl Write for BatchedHashWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > BATCH_BYTES - self.len {
            self.hash_batch();
        }
        if bytes.len() >= BATCH_BYTES {
            // Already a large update: hashing it in place avoids copying it.
            self.state.update(bytes);
        } else {
            self.batch[self.len..self.len + bytes.len()].copy_from_slice(bytes);
            self.len += bytes.len();
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::serialization::ZcashDeserializeInto;

    fn blake2b_256(bytes: &[u8]) -> [u8; 32] {
        let mut digest = [0; 32];
        digest.copy_from_slice(Params::new().hash_length(32).hash(bytes).as_bytes());
        digest
    }

    /// Writes that fill, straddle, exceed and exactly match a batch hash the same bytes as
    /// one call over all of them.
    #[test]
    fn batching_preserves_the_digest() {
        let sizes = [
            0,
            1,
            BATCH_BYTES - 1,
            1,
            BATCH_BYTES,
            2,
            BATCH_BYTES + 1,
            BATCH_BYTES - 2,
            3 * BATCH_BYTES,
            7,
        ];
        let mut writer = BatchedHashWriter::new();
        let mut all = Vec::new();
        for (index, size) in sizes.into_iter().enumerate() {
            // Truncating to the low byte is intended: any varied byte pattern will do.
            let bytes: Vec<u8> = (0..size).map(|offset| (index + offset) as u8).collect();
            writer.write_all(&bytes).unwrap();
            all.extend_from_slice(&bytes);
        }
        assert_eq!(writer.finalize(), blake2b_256(&all));
        assert_eq!(BatchedHashWriter::new().finalize(), blake2b_256(&[]));
    }

    /// Every test vector block's digests equal hashes of its serialized bytes.
    #[test]
    fn digests_hash_the_serialized_block() {
        for bytes in zakura_test::vectors::BLOCKS.iter() {
            let block: Block = bytes.zcash_deserialize_into().unwrap();
            let body = BodyDigest::of(&block);
            assert_eq!(
                body.0,
                blake2b_256(&block.transactions.zcash_serialize_to_vec().unwrap())
            );

            let mut header_and_body = block.header.zcash_serialize_to_vec().unwrap();
            header_and_body.extend_from_slice(&body.0);
            assert_eq!(
                body.with_header(&block.header),
                blake2b_256(&header_and_body)
            );
        }
    }
}
