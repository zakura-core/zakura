//! Properties shared across reactors (properties.md, Properties shared
//! across reactors). Each file defines one idea once. A reactor registers
//! an instance of it under its own property ID.
//!
//! `Property::check` is a `fn` pointer, so a check can't capture which
//! instance it serves. Each idea is a trait plus one generic constructor,
//! and each instance is a marker type that implements the trait for its
//! harness's view. The shared check reports through `cx.id()`, so one check
//! serves every instance's ID.

pub mod bounds;
