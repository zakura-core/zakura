//! Runtime names for Zakura's native P2P stack.
//!
//! The v2 P2P code in `zakura-network` imports its tasks, timers, sync
//! primitives, hashed collections and thread RNG from this crate rather than
//! from `tokio`, `std` or `rand`. `cargo xtask check-rt-imports` enforces that.
//!
//! In a production build every name here is a re-export of the item the code
//! used before, so routing through this crate changes no behavior. The
//! deterministic simulator will give these names its own implementations, and
//! the list below is the surface it must cover.
//!
//! The simulator build is `RUSTFLAGS="--cfg zakura_sim"`. It swaps in the
//! modules under `src/sim/` and adds `sim`, the interface the simulator's
//! scheduler implements. Names it doesn't cover yet stay re-exports.

pub use tokio::{pin, select};

#[cfg(zakura_sim)]
pub mod sim;

#[cfg(zakura_sim)]
#[path = "sim/task.rs"]
pub mod task;

#[cfg(zakura_sim)]
#[path = "sim/time.rs"]
pub mod time;

#[cfg(zakura_sim)]
#[path = "sim/sync/mod.rs"]
pub mod sync;

/// Tasks.
#[cfg(not(zakura_sim))]
pub mod task {
    pub use tokio::task::{
        coop, spawn, spawn_blocking, yield_now, AbortHandle, JoinHandle, JoinSet,
    };
    pub use tokio_util::task::AbortOnDropHandle;
}

/// Time.
///
/// `Instant` is tokio's. Code that uses `std::time::Instant` keeps it for now,
/// because tokio's `Instant` behaves differently under paused test time.
#[cfg(not(zakura_sim))]
pub mod time {
    pub use tokio::time::{
        error, interval, sleep, sleep_until, timeout, Instant, MissedTickBehavior, Sleep, Timeout,
    };
}

/// Synchronization.
///
/// `Mutex` is the std mutex. Tokio's mutex is `AsyncMutex`.
#[cfg(not(zakura_sim))]
pub mod sync {
    pub use std::sync::{atomic, Mutex, MutexGuard, PoisonError};
    pub use tokio::sync::{
        broadcast, futures, mpsc, oneshot, watch, Barrier, Mutex as AsyncMutex, Notify,
        OwnedSemaphorePermit, Semaphore,
    };
    pub use tokio_util::sync::CancellationToken;

    /// Metrics-only atomics: plain std atomics, with no preemption points, in
    /// both builds. Use [`atomic`] for anything another task reads to decide
    /// what to do.
    pub mod counter {
        pub use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    }
}

/// Hashed collections, whose iteration order depends on a random seed.
pub mod collections {
    pub use std::collections::{hash_map, HashMap, HashSet};
}

/// Streams.
pub mod stream {
    pub use tokio_stream::StreamMap;
}

/// Randomness.
///
/// Only the thread RNG lives here. Code keeps importing the RNG traits from
/// `rand`.
pub mod rand {
    pub use rand_10::rng;
}
