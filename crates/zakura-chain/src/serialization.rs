//! Read and write the byte formats Zcash uses for blocks, transactions, and messages.
//!
//! This module contains four traits: `ZcashSerialize` and `ZcashDeserialize`,
//! analogs of the Serde `Serialize` and `Deserialize` traits but intended for
//! consensus-critical Zcash serialization formats, and `WriteZcashExt` and
//! `ReadZcashExt`, extension traits for `io::Read` and `io::Write` with utility functions
//! for reading and writing data (e.g., the Bitcoin variable-integer format).
//!
//! A message can declare more items than it supplies. When the complete payload
//! is in memory, use [`ZcashDeserialize::zcash_deserialize_from_slice`]. Its
//! [`ZcashReader`] carries the remaining byte count through nested decoders.
//! Collections must fit that input and the protocol's size limits.
//!
//! [`TrustedPreallocate::min_serialized_size`] must allow every supported encoding,
//! including Regtest's smaller headers. Consensus validation remains separate.
//!
//! Implementations use [`ZcashReader::read_value`],
//! [`ZcashReader::read_external_count`], and [`ZcashReader::read_bytes`] for nested
//! data. Older streaming entry points remain supported, but cannot infer how
//! many bytes are actually available from a maximum read limit.

mod compact_size;
mod constraint;
mod date_time;
mod error;
mod read_zcash;
mod reader;
mod write_zcash;
mod zcash_deserialize;
mod zcash_serialize;

pub mod display_order;
pub mod serde_adapters;
pub mod sha256d;

pub(crate) mod serde_helpers;

#[cfg(any(test, feature = "proptest-impl"))]
pub mod arbitrary;

#[cfg(test)]
pub mod tests;

pub use compact_size::{CompactSize64, CompactSizeMessage};
pub use constraint::AtLeastOne;
pub use date_time::{DateTime32, Duration32};
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
