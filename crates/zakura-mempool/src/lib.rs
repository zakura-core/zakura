//! Zakura's pool core: a pure state machine, its storage and policy, and the
//! one task that runs it behind the port in
//! `zakura_node_services::mempool::port`.
//!
//! The machine and its storage emit no metrics and no logs. The task reads
//! their state after each step and emits the metrics.
//!
//! Storage is generic over the verification error it caches, so this crate
//! does not depend on the transaction verifier.

pub mod config;
pub mod error;
pub mod io;
pub mod machine;
pub mod storage;

#[cfg(test)]
mod arbitrary;

pub use config::Config;
pub use error::MempoolError;
pub use storage::{
    EffectsRejection, ExactTipRejectionError, NonStandardTransactionError, RejectionError,
    SameEffectsChainRejectionError, SameEffectsTipRejectionError, Storage, StorageStats,
};

/// The instant time is read as. In a production build this is tokio's
/// instant, and the simulator gives it its own clock.
pub use zakura_rt::time::Instant;
