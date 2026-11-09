//! Cumulative admission of transparent verification work for one block attempt.

use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use super::TransactionError;
use zakura_chain::transaction::zip317::MAX_BLOCK_VERIFICATION_WORK;

/// Shared, monotonic budget for a single block verification attempt.
///
/// Reservations are never refunded: this is a consensus total, not a concurrency
/// limit. Request equality compares budget identity, not its changing balance.
#[derive(Clone, Debug)]
pub struct VerificationBudget(Arc<AtomicU32>);

impl Default for VerificationBudget {
    fn default() -> Self {
        Self(Arc::new(AtomicU32::new(MAX_BLOCK_VERIFICATION_WORK)))
    }
}

impl PartialEq for VerificationBudget {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for VerificationBudget {}

impl VerificationBudget {
    /// Reserve all of a transaction's work before dispatching its cryptography.
    pub(super) fn reserve(&self, work: u32) -> Result<(), TransactionError> {
        self.0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(work)
            })
            .map(|_| ())
            .map_err(|_| TransactionError::VerificationWorkLimit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_budget_accepts_exact_limit_without_refunding() {
        let budget = VerificationBudget::default();
        let other = budget.clone();
        budget.reserve(MAX_BLOCK_VERIFICATION_WORK - 1).unwrap();
        other.reserve(1).unwrap();
        assert_eq!(
            budget.reserve(1),
            Err(TransactionError::VerificationWorkLimit)
        );
        assert_eq!(
            other.reserve(u32::MAX),
            Err(TransactionError::VerificationWorkLimit)
        );
        budget.reserve(0).unwrap();
    }
}
