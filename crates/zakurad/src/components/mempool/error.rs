//! Errors that can occur when interacting with the mempool.
//!
//! Both pools share the pool core's error types, so callers downcast to one
//! type whichever pool is running.

/// Mempool errors, with the consensus verifier's error in a cached rejection.
pub type MempoolError = zakura_mempool::MempoolError<zakura_consensus::error::TransactionError>;
