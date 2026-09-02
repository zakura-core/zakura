//! Executable contract evidence for the GetBlocks wire-contract pilot.
//!
//! The oracle stays separate from the production codec. Shared reporting belongs
//! in [`support`]; it must not become another validation layer.

mod get_blocks;
mod support;
