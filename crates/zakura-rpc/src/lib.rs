//! The Zakura node's Remote Procedure Call (RPC) interface

#![doc(html_favicon_url = "https://zakura.com/assets/rustdoc/zakura-favicon-128.png")]
#![doc(html_logo_url = "https://zakura.com/assets/rustdoc/zakura-icon.png")]
#![doc(html_root_url = "https://docs.rs/zakura_rpc")]
// Long Tower service and future types are routine in this crate, and factoring
// them into type aliases would not make the code clearer.
#![allow(clippy::type_complexity)]

pub mod client;
pub mod config;
pub mod indexer;
pub mod methods;
pub mod queue;
pub mod server;
pub mod sync;

#[cfg(test)]
mod tests;

pub use methods::types::{
    get_block_template::{fetch_chain_info, proposal::proposal_block_from_template, MinerParams},
    submit_block::SubmitBlockChannel,
};
