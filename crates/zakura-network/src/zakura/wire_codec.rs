//! Typed, bounded codecs for Zakura message payloads.
//!
//! A message's payload is one [`Wire`] item. Items compose: a tuple of items is
//! an item, and a [`List`] of items is an item. Each item states its shortest
//! and longest encoding and the most heap its decoder may request, so a
//! payload's bounds follow from its parts:
//!
//! ```
//! # use zakura_network::zakura::{wire_codec::{HeightLe, List, U8}, PayloadLen};
//! // A height, then one to 1024 bytes.
//! type ItemPayload = (HeightLe, List<U8, 1, 1024>);
//! assert_eq!(PayloadLen::of::<ItemPayload>(), PayloadLen::between(6, 1031));
//! ```
//!
//! A message family implements [`WireMessage`] once, as plain `match`es from
//! message type to payload item. [`encode_frame`] and [`decode_frame`] check
//! each frame against the family's rows, decode it, and reject trailing bytes.
//!
//! Tests check each item once, and each family against its rows, with the
//! suites in `item_suite` and `message_suite`. A message that composes checked
//! items needs no bound test of its own.

mod error;
mod item;
mod list;
mod message;
mod reader;

#[cfg(test)]
pub(crate) mod item_suite;
#[cfg(test)]
pub(crate) mod message_suite;
#[cfg(test)]
pub(crate) mod sample;
#[cfg(test)]
mod tests;

pub use error::WireError;
pub use item::{HeightLe, LeU32, Wire, U8};
pub use list::List;
pub use message::{decode_frame, encode_frame, WireMessage};
pub use reader::BoundedReader;
