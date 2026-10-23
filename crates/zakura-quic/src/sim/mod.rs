//! Deterministic simulation support: [`SimNet`], an in-memory network behind
//! the socket seam.
//!
//! Only `cfg(any(test, zakura_sim))` builds compile this module. Release
//! builds never set `zakura_sim`, so `zakurad` never contains it.

mod net;

pub use net::{DatagramId, DropReason, Link, LinkModel, NetError, NetEvent, SimNet, SimSocket};
