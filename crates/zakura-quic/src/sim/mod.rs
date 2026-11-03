//! Deterministic simulation support: [`SimNet`], an in-memory network behind
//! the socket seam, and [`SeededInputs`], which derives every
//! [`crate::EndpointInputs`] source and the node key from one seed.
//!
//! Only `cfg(any(test, zakura_sim))` builds compile this module. Release
//! builds never set `zakura_sim`, so `zakurad` never contains it, and it uses
//! only dependencies DEP-2 already lists.

mod net;
mod seeded;

pub use net::{DatagramId, DropReason, Link, LinkModel, NetError, NetEvent, SimNet, SimSocket};
pub use seeded::{InputRecord, SeededInputs, Source};
