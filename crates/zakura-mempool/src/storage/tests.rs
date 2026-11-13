//! Tests for mempool storage.

mod eviction;
mod known_ids;
mod mutation_benchmarks;
mod prop;
mod vectors;

/// The verifier's error, as these tests see it.
#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
#[error("the test verifier rejected the transaction")]
pub struct TestVerifyError;

/// Storage that caches [`TestVerifyError`].
pub type Storage = super::Storage<TestVerifyError>;

/// A mempool error that can hold [`TestVerifyError`].
pub type MempoolError = crate::MempoolError<TestVerifyError>;

/// An exact-tip rejection that can hold [`TestVerifyError`].
pub type ExactTipRejectionError = super::ExactTipRejectionError<TestVerifyError>;

/// A rejection that can hold [`TestVerifyError`].
pub type RejectionError = super::RejectionError<TestVerifyError>;
