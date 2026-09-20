//! Receipt priority shared by overlapping deliveries of the same complete block.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use zakura_chain::block::{self, Block};

static NEXT_RECEIPT_ORDER: AtomicU64 = AtomicU64::new(1);

/// Tracks complete bodies for active receipts. The last caller removes its entry
/// on completion or cancellation, so no receipt history is retained.
#[derive(Debug, Default)]
pub(super) struct ReceiptRegistry(Arc<Mutex<HashMap<block::Hash, Vec<Entry>>>>);

#[derive(Debug)]
struct Entry {
    block: Arc<Block>,
    order: u64,
    callers: usize,
}

/// Keeps the first receipt registered until every overlapping call has finished.
pub(super) struct ReceiptGuard {
    registry: Arc<Mutex<HashMap<block::Hash, Vec<Entry>>>>,
    hash: block::Hash,
    pub(super) order: u64,
}

impl ReceiptRegistry {
    /// Called synchronously by the verifier before returning its request future.
    pub(super) fn register(&self, block: Arc<Block>) -> ReceiptGuard {
        let hash = block.hash();
        let mut registry = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entries = registry.entry(hash).or_default();
        // Header hashes alone do not identify unvalidated bodies. A bad body
        // must not give a later valid body its receipt priority.
        let order = if let Some(entry) = entries.iter_mut().find(|entry| entry.block == block) {
            entry.callers = entry
                .callers
                .checked_add(1)
                .expect("each active caller owns memory, so its count fits in usize");
            entry.order
        } else {
            let order = NEXT_RECEIPT_ORDER
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |order| {
                    order.checked_add(1)
                })
                .expect("a process cannot receive u64::MAX blocks");
            entries.push(Entry {
                block,
                order,
                callers: 1,
            });
            order
        };
        ReceiptGuard {
            registry: self.0.clone(),
            hash,
            order,
        }
    }
}

impl Drop for ReceiptGuard {
    fn drop(&mut self) {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entries = registry
            .get_mut(&self.hash)
            .expect("a live guard has a registered block");
        let index = entries
            .iter()
            .position(|entry| entry.order == self.order)
            .expect("a live guard has a registered receipt");
        entries[index].callers -= 1;
        if entries[index].callers == 0 {
            entries.swap_remove(index);
        }
        if entries.is_empty() {
            registry.remove(&self.hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::serialization::ZcashDeserializeInto;

    #[test]
    fn overlapping_receipts_share_priority_and_release_all_entries() {
        let registry = ReceiptRegistry::default();
        let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let first = registry.register(block.clone());
        let original_order = first.order;
        let duplicate = registry.register(Arc::new(block.as_ref().clone()));
        assert_eq!(duplicate.order, original_order);
        drop(first);
        let overlap = registry.register(block.clone());
        assert_eq!(overlap.order, original_order);

        let mut malformed = block.as_ref().clone();
        malformed.transactions.clear();
        assert_eq!(malformed.hash(), block.hash());
        let malformed = registry.register(Arc::new(malformed));
        assert!(malformed.order > original_order);
        drop((duplicate, overlap, malformed));
        assert!(registry.0.lock().unwrap().is_empty());

        let retry = registry.register(block);
        assert!(retry.order > original_order);
        drop(retry);
        assert!(registry.0.lock().unwrap().is_empty());
    }
}
