//! Executable candidate contracts for the two block-sync pilot messages.
//!
//! Keep message-specific oracles and generators in their named modules. Shared
//! reporting belongs in [`support`]; it must not become a second production
//! codec or validation layer.

mod get_blocks;
mod status;
mod support;
