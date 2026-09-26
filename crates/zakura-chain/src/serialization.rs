//! Read and write the byte formats Zcash uses for blocks, transactions, and messages.
//!
//! This module contains four traits: `ZcashSerialize` and `ZcashDeserialize`,
//! analogs of the Serde `Serialize` and `Deserialize` traits but intended for
//! consensus-critical Zcash serialization formats, and `WriteZcashExt` and
//! `ReadZcashExt`, extension traits for `io::Read` and `io::Write` with utility functions
//! for reading and writing data (e.g., the Bitcoin variable-integer format).
//!
//! A message can declare more items than it actually supplies. When all payload
//! bytes are in memory, configure a [`ZcashDecoder`] for the node's network and
//! reuse it for each payload. Its [`ZcashReader`] tells nested decoders how many
//! bytes remain and which network rules apply. Collections must fit both that
//! input and the protocol's size limits.
//!
//! [`TrustedPreallocate::min_serialized_size_for`] gives the smallest number of
//! bytes needed to decode one item under those rules. Regtest has smaller headers
//! than Mainnet and Testnet, so their decoders must not use its smaller minimum.
//! This is a parsing check. It still accepts values that later fail consensus
//! validation.
//!
//! Implementations keep their field parsing in
//! [`ZcashDeserialize::zcash_deserialize_from`] and use [`ZcashReader::read_value`],
//! [`ZcashReader::read_external_count`], and [`ZcashReader::read_bytes`] for nested
//! data. Calling the older `io::Read` entry points inside a bounded decoder loses
//! the remaining byte count and the configured network rules.
//! Existing streaming callers and implementations remain supported, but an
//! unknown stream length cannot reject a count based on bytes available. Generic
//! decoding methods accept encodings from any network for offline use.

mod compact_size;
mod constraint;
mod date_time;
mod decoder;
mod error;
mod read_zcash;
mod reader;
mod write_zcash;
mod zcash_deserialize;
mod zcash_serialize;

pub mod display_order;
pub mod sha256d;

pub(crate) mod serde_helpers;

#[cfg(any(test, feature = "proptest-impl"))]
pub mod arbitrary;

#[cfg(test)]
pub mod tests;

pub use compact_size::{CompactSize64, CompactSizeMessage};
pub use constraint::AtLeastOne;
pub use date_time::{DateTime32, Duration32};
pub use decoder::ZcashDecoder;
pub use display_order::BytesInDisplayOrder;
pub use error::SerializationError;
pub use read_zcash::ReadZcashExt;
pub use reader::ZcashReader;
pub use write_zcash::WriteZcashExt;
pub use zcash_deserialize::{
    zcash_deserialize_bytes_external_count, zcash_deserialize_external_count,
    zcash_deserialize_string_external_count, TrustedPreallocate, ZcashDeserialize,
    ZcashDeserializeInto,
};
pub use zcash_serialize::{
    zcash_serialize_bytes, zcash_serialize_bytes_external_count, zcash_serialize_empty_list,
    zcash_serialize_external_count, FakeWriter, ZcashSerialize, MAX_HEADERS_PER_MESSAGE,
    MAX_PROTOCOL_MESSAGE_LEN,
};
