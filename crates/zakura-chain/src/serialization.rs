//! Consensus-critical serialization.
//!
//! This module contains four traits: `ZcashSerialize` and `ZcashDeserialize`,
//! analogs of the Serde `Serialize` and `Deserialize` traits but intended for
//! consensus-critical Zcash serialization formats, and `WriteZcashExt` and
//! `ReadZcashExt`, extension traits for `io::Read` and `io::Write` with utility functions
//! for reading and writing data (e.g., the Bitcoin variable-integer format).
//!
//! Decode complete, untrusted payloads with
//! [`ZcashDeserialize::zcash_deserialize_from_slice`]. Its [`ZcashReader`] carries
//! the actual remaining bytes through nested values and size limits. Collection
//! decoders reject counts that exceed the protocol cap or cannot fit in that
//! input before reserving memory. [`TrustedPreallocate::min_serialized_size`]
//! describes the smallest encoding accepted by the element decoder, including
//! structurally valid values that later consensus checks might reject.
//!
//! Implementations keep their field parsing in
//! [`ZcashDeserialize::zcash_deserialize_from`] and use the reader's nested-value,
//! external-count, and byte-string methods to preserve these bounds. Calling the
//! older `io::Read` entry points inside a bounded decoder loses that guarantee.
//! Existing streaming callers and implementations remain supported, but an
//! unknown-length stream cannot provide an actual-input preallocation bound.

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
