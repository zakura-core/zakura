//! Zakura's transaction pool.
//!
//! This crate holds the pool's storage and its policy, which both pools use,
//! and the pool core: a pure state machine and the one task that runs it
//! behind the port in `zakura_node_services::mempool::port`.
//!
//! Storage is generic over the verification error it caches, so this crate
//! does not depend on the transaction verifier.

pub mod config;
pub mod error;
pub mod io;
pub mod machine;
pub mod peer_cooldown;
pub mod storage;

#[cfg(any(test, feature = "proptest-impl"))]
pub mod arbitrary;

pub use config::Config;
pub use error::MempoolError;
pub use storage::{
    ExactTipRejectionError, NonStandardTransactionError, RejectionError, RemovedTransactionIds,
    SameEffectsChainRejectionError, SameEffectsTipRejectionError, Storage,
};

/// The instant time is read as. In a production build this is tokio's
/// instant, and the simulator gives it its own clock.
pub use zakura_rt::time::Instant;
