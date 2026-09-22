//! Typed, bounded codecs for Zakura message payloads.
//!
//! A message family implements [`WireMessage`] once. Its fields use [`Wire`]
//! items and [`BoundedVec`] list descriptors, so each list count is checked
//! against the remaining payload before the list is allocated. The family's
//! rule table bounds each payload before decoding starts.
//!
//! In test builds, [`conformance`] checks any family against its rule table
//! and [`allocation_meter`] measures heap use during a decode.

mod bounded_reader;
mod bounded_vec;
mod wire_error;
mod wire_items;
mod wire_message;

#[cfg(test)]
pub(crate) mod allocation_meter;
#[cfg(test)]
pub(crate) mod conformance;
#[cfg(test)]
mod tests;

pub use bounded_reader::BoundedReader;
pub use bounded_vec::BoundedVec;
pub use wire_error::WireError;
pub use wire_items::{HashItem, HeightLe, LeU32, Wire, Zcash};
pub use wire_message::{decode_frame, decode_payload_exact, encode_frame, WireMessage};
