//! The pool's storage, which lives in `zakura-mempool`, with the verifier's
//! error filled in.

pub use zakura_mempool::storage::*;

use zakura_consensus::error::TransactionError;

/// Storage that caches the verifier's errors.
pub type Storage = zakura_mempool::Storage<TransactionError>;

/// An exact-tip rejection that can hold the verifier's error.
pub type ExactTipRejectionError = zakura_mempool::ExactTipRejectionError<TransactionError>;
