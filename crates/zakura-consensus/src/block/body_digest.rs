//! A digest of a block's serialized transactions, shared by the block identities the verifier
//! derives.

use blake2b_simd::Params;
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
        let mut state = Params::new().hash_length(32).to_state();
        block
            .transactions
            .zcash_serialize(&mut state)
            .expect("serializing parsed transactions to a hash cannot fail");
        let mut digest = [0; 32];
        digest.copy_from_slice(state.finalize().as_bytes());
        Self(digest)
    }

    /// Blake2b-256 of `header`'s serialization followed by this digest.
    pub(super) fn with_header(&self, header: &Header) -> [u8; 32] {
        let mut state = Params::new().hash_length(32).to_state();
        header
            .zcash_serialize(&mut state)
            .expect("serializing a header to a hash cannot fail");
        state.update(&self.0);
        let mut digest = [0; 32];
        digest.copy_from_slice(state.finalize().as_bytes());
        digest
    }
}
