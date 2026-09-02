//! Executable contract evidence for the GetBlocks wire-contract pilot.
//!
//! The oracle stays separate from the production codec. Shared reporting belongs
//! in [`support`]; it must not become another validation layer.

mod get_blocks;
mod get_blocks_serving;
mod get_blocks_serving_api;
mod support;
