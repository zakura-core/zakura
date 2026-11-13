//! Errors that can occur when interacting with the mempool.

use zakura_consensus::error::TransactionError;

/// Mempool errors, with the verifier's error filled in.
pub type MempoolError = zakura_mempool::MempoolError<TransactionError>;
