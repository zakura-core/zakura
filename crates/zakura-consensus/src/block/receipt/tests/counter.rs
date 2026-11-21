//! The receipt order counter.

use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

#[test]
fn receipt_order_never_wraps_on_exhaustion() {
    let counter = AtomicU64::new(u64::MAX - 1);
    assert_eq!(order::next_from(&counter), u64::MAX - 1);
    assert!(std::panic::catch_unwind(|| order::next_from(&counter)).is_err());
    assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
}

#[test]
fn concurrent_receipt_orders_are_unique() {
    let counter = AtomicU64::new(1);
    let orders = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    (0..100)
                        .map(|_| order::next_from(&counter))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect::<std::collections::HashSet<_>>()
    });
    assert_eq!(orders.len(), 800);
    assert!(orders.contains(&1));
    assert!(orders.contains(&800));
    assert_eq!(counter.load(Ordering::Relaxed), 801);
}
