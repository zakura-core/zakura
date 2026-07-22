//! Typed transition evidence, durable snapshots, and read-oriented store contracts.

mod store;
mod types;

pub use store::{StoreError, StoreRead};
pub use types::*;
