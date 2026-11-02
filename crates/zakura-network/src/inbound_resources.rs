//! Node-wide owners for work that legacy TCP and compatibility peers can start.
//!
//! # Capacity and progress contract
//!
//! Nothing in this module waits, so no caller can wait for capacity while it holds a
//! lock or an earlier reservation. Each limit acts only on work that a conformant
//! peer cannot be waiting for, or on a count that conformant callers cannot reach:
//!
//! | Owner | Refusal |
//! | --- | --- |
//! | [`InboundBudget::try_charge`] | Unsolicited work drops, as it does when a queue is full. |
//! | [`InboundBudget::charge_committed`] | Never refuses. A response the node asked for cannot wait or drop without timing out a conformant peer. |
//! | [`TcpHandshakeOwner`] | Sized for every connection slot, so a refusal means a dialer exceeded its own limit. |

use std::sync::Arc;

use zakura_rt::sync::{
    atomic::{AtomicU64, Ordering},
    OwnedSemaphorePermit, Semaphore,
};

/// Decoded `block` and `tx` bytes that unsolicited TCP work may retain across this node.
///
/// A connection handles one message at a time, so this bound applies only to a node
/// whose connections all hold large transactions at once. This is an unmeasured choice,
/// and it matches the compatibility partition.
pub(crate) const TCP_INBOUND_RETAINED_BYTES: u64 = 128 * 1024 * 1024;

/// The heap bytes of a vector's own allocation.
pub(crate) fn vec_retained_bytes<T>(items: &Vec<T>) -> u64 {
    // Widening usize to u64 is lossless on supported targets.
    items.capacity().saturating_mul(std::mem::size_of::<T>()) as u64
}

/// Decoded bytes one budget's unsolicited work may retain.
///
/// This is one partition of the node's decoded ingress. Each budget has one owner, so
/// a flood on one protocol cannot refuse a conformant request on the other.
#[derive(Clone, Debug)]
pub(crate) struct InboundBudget {
    used: Arc<AtomicU64>,
    capacity: u64,
}

impl InboundBudget {
    /// Create one budget. Clones share its capacity.
    pub(crate) fn new(capacity: u64) -> Self {
        Self {
            used: Arc::new(AtomicU64::new(0)),
            capacity,
        }
    }

    /// Charge `bytes` now, or nothing. A larger item clamps to the whole budget, so
    /// it can still enter an idle budget. An empty item always enters.
    pub(crate) fn try_charge(&self, bytes: u64) -> Option<InboundCharge> {
        let bytes = bytes.min(self.capacity);
        if bytes == 0 {
            return Some(self.charge_of(0));
        }
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.capacity)
            })
            .ok()
            .map(|_| self.charge_of(bytes))
    }

    /// Charge the exact `bytes` of work the node already committed to, even above the
    /// capacity. Unsolicited work then refuses until this charge ends.
    ///
    /// The caller bounds this work itself. A TCP connection has one outstanding
    /// request and serves one request at a time.
    pub(crate) fn charge_committed(&self, bytes: u64) -> InboundCharge {
        let _ = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                Some(used.saturating_add(bytes))
            });
        self.charge_of(bytes)
    }

    fn charge_of(&self, bytes: u64) -> InboundCharge {
        InboundCharge {
            used: self.used.clone(),
            bytes,
        }
    }

    /// Bytes currently charged.
    #[cfg(test)]
    pub(crate) fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    /// Bytes that unsolicited work can still charge.
    #[cfg(test)]
    pub(crate) fn available(&self) -> u64 {
        self.capacity.saturating_sub(self.used())
    }
}

/// Bytes charged to one [`InboundBudget`] until this charge drops.
#[derive(Debug)]
pub(crate) struct InboundCharge {
    used: Arc<AtomicU64>,
    bytes: u64,
}

impl InboundCharge {
    /// Combine `other`, which must come from this charge's budget.
    pub(crate) fn merge(&mut self, mut other: InboundCharge) {
        debug_assert!(Arc::ptr_eq(&self.used, &other.used));
        self.bytes = self.bytes.saturating_add(other.bytes);
        other.bytes = 0;
    }
}

impl Drop for InboundCharge {
    fn drop(&mut self) {
        let _ = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                Some(used.saturating_sub(self.bytes))
            });
    }
}

/// Aggregate owner of TCP handshakes that have not finished.
///
/// Each handshake holds one slot from its first byte until it ends or times out. A
/// slot bounds the handshake's codec buffer by the pre-handshake body limit, its
/// decoded `version` by the user-agent limit, and its phase metadata by one task.
/// The inbound and outbound connection counters reserve a connection before its
/// handshake starts, so a slot per connection cannot refuse a conformant peer.
#[derive(Clone, Debug)]
pub(crate) struct TcpHandshakeOwner {
    slots: Arc<Semaphore>,
}

impl TcpHandshakeOwner {
    /// Create an owner with one slot per connection, and at least one slot.
    pub(crate) fn new(connection_limit: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(connection_limit.max(1))),
        }
    }

    /// Take a slot now, or nothing.
    pub(crate) fn try_admit(&self) -> Option<OwnedSemaphorePermit> {
        self.slots.clone().try_acquire_owned().ok()
    }

    /// Slots no handshake holds.
    #[cfg(test)]
    pub(crate) fn available(&self) -> usize {
        self.slots.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A budget admits within its capacity and refuses without changing its charge.
    #[test]
    fn a_saturated_budget_refuses_unsolicited_work() {
        let budget = InboundBudget::new(10);
        let held = budget.try_charge(6).expect("the budget is idle");
        assert!(budget.try_charge(5).is_none());
        assert_eq!(budget.used(), 6);
        drop(held);
        assert_eq!(budget.used(), 0);
    }

    /// A committed charge exceeds the capacity instead of refusing or waiting, and
    /// unsolicited work refuses until the committed charge ends.
    #[test]
    fn committed_work_is_charged_above_the_capacity() {
        let budget = InboundBudget::new(10);
        let mut committed = budget.charge_committed(25);
        committed.merge(budget.charge_committed(5));
        assert_eq!(budget.used(), 30);
        assert_eq!(budget.available(), 0);
        assert!(budget.try_charge(1).is_none());
        assert!(
            budget.try_charge(0).is_some(),
            "an empty item always enters"
        );
        drop(committed);
        assert_eq!(budget.used(), 0);
        assert!(budget.try_charge(1).is_some());
    }

    /// L.handshake: the owner counts every unfinished handshake across its clones,
    /// and a zero connection limit still leaves one slot for isolated use.
    #[test]
    fn the_handshake_owner_bounds_handshakes_in_aggregate() {
        let owner = TcpHandshakeOwner::new(2);
        let clone = owner.clone();
        let first = owner.try_admit().expect("one of two slots");
        let _second = clone.try_admit().expect("two of two slots");
        assert!(owner.try_admit().is_none());
        assert_eq!(clone.available(), 0);
        drop(first);
        assert_eq!(owner.available(), 1);

        assert_eq!(TcpHandshakeOwner::new(0).available(), 1);
    }
}
