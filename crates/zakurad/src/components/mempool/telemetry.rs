//! Opt-in lifecycle observations without publishing transaction IDs or contents.

use std::{
    collections::hash_map::RandomState,
    hash::BuildHasher,
    sync::{
        atomic::{AtomicU64, Ordering},
        OnceLock,
    },
};
use zakura_chain::transaction::UnminedTxId;

static NEXT_ATTEMPT: AtomicU64 = AtomicU64::new(1);

static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();

fn token(id: UnminedTxId) -> String {
    let keys = KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]);
    format!("{:016x}{:016x}", keys[0].hash_one(id), keys[1].hash_one(id))
}

/// Correlate an exact witnessed ID within this node run using a salted token.
/// Labels must be fixed public categories, never raw validation errors.
pub(super) fn emit(id: UnminedTxId, phase: &'static str, reason: Option<&'static str>) {
    emit_inner(id, None, phase, reason);
}

/// Allocate an occurrence identity for one lifecycle operation.
pub(super) fn new_attempt() -> u64 {
    NEXT_ATTEMPT.fetch_add(1, Ordering::Relaxed)
}

/// Emit a boundary tied to a particular lifecycle operation.
pub(super) fn emit_attempt(
    id: UnminedTxId,
    attempt: u64,
    phase: &'static str,
    reason: Option<&'static str>,
) {
    emit_inner(id, Some(attempt), phase, reason);
}

fn emit_inner(
    id: UnminedTxId,
    attempt: Option<u64>,
    phase: &'static str,
    reason: Option<&'static str>,
) {
    zakura_jsonl_trace::dashboard::emit(|| {
        serde_json::json!({
            "event": "transaction_lifecycle", "transaction": token(id),
            "phase": phase, "reason": reason, "attempt": attempt,
        })
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn token_is_stable_without_exposing_the_id() {
        let first = UnminedTxId::Legacy([1; 32].into());
        let other = UnminedTxId::Legacy([2; 32].into());
        assert_eq!(token(first), token(first));
        assert_ne!(token(first), token(other));
        assert_eq!(token(first).len(), 32);
        assert_ne!(token(first), "01".repeat(16));
    }
}
