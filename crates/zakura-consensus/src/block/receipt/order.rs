//! The process-wide receipt order, shared by claims and receipts so the two compare.

use std::sync::atomic::{AtomicU64, Ordering};

use tokio::time::Instant;

/// A complete body's own place and deadline reference, captured before either receipt lock.
#[derive(Clone, Copy)]
pub(super) struct BodyArrival {
    pub(super) order: u64,
    pub(super) time: Instant,
}

impl BodyArrival {
    pub(super) fn now() -> Self {
        Self {
            order: next(),
            time: Instant::now(),
        }
    }
}

static NEXT_RECEIPT_ORDER: AtomicU64 = AtomicU64::new(1);

/// Draws the next receipt order.
pub(super) fn next() -> u64 {
    next_from(&NEXT_RECEIPT_ORDER)
}

/// Draws the next order from `counter`. Orders are unique and never wrap.
pub(super) fn next_from(counter: &AtomicU64) -> u64 {
    let mut order = counter.load(Ordering::Relaxed);
    loop {
        let next = order
            .checked_add(1)
            .expect("a process cannot receive u64::MAX blocks");
        match counter.compare_exchange_weak(order, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return order,
            Err(current) => order = current,
        }
    }
}
