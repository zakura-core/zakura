//! The property registry (properties.md, Registry).

mod id;
mod registry;

pub use id::{BadId, PropertyId};
pub use registry::{CheckEvent, Lifecycle, Tee};
