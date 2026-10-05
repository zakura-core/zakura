//! Receipt priority for concurrent deliveries of complete blocks.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use zakura_chain::{block::Block, serialization::ZcashSerialize};

static NEXT_RECEIPT_ORDER: AtomicU64 = AtomicU64::new(1);

/// Overlapping deliveries of the same complete block share its first receipt.
/// Entries live only while a verification of that body is active, so a block
/// delivered again after every attempt finishes or is cancelled gets a new receipt.
#[derive(Clone, Debug, Default)]
pub(super) struct ReceiptRegistry(Arc<Mutex<HashMap<[u8; 32], Entry>>>);

#[derive(Debug)]
struct Entry {
    order: u64,
    callers: usize,
}

/// Keeps a block's receipt active until its verification finishes or is cancelled.
pub(super) struct ReceiptGuard {
    registry: Arc<Mutex<HashMap<[u8; 32], Entry>>>,
    body_digest: [u8; 32],
    pub(super) order: u64,
}

impl ReceiptRegistry {
    /// Called synchronously by the verifier before returning its request future.
    pub(super) fn register(&self, block: Arc<Block>) -> ReceiptGuard {
        // Hash the complete serialized body before locking. ZIP-244 permits
        // unequal authorizing data under the same header and transaction IDs.
        let mut digest = blake2b_simd::Params::new().hash_length(32).to_state();
        block
            .zcash_serialize(&mut digest)
            .expect("serializing a parsed block to a hash cannot fail");
        let body_digest = digest
            .finalize()
            .as_bytes()
            .try_into()
            .expect("the digest is configured for 32 bytes");
        let mut active = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let order = if let Some(entry) = active.get_mut(&body_digest) {
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
            active.insert(body_digest, Entry { order, callers: 1 });
            order
        };
        ReceiptGuard {
            registry: self.0.clone(),
            body_digest,
            order,
        }
    }
}

impl Drop for ReceiptGuard {
    fn drop(&mut self) {
        let mut active = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = active
            .get_mut(&self.body_digest)
            .expect("a live guard has a registered receipt");
        entry.callers -= 1;
        if entry.callers == 0 {
            active.remove(&self.body_digest);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::serialization::ZcashDeserializeInto;

    #[test]
    fn receipts_retain_identity_without_retaining_full_bodies() {
        let registry = ReceiptRegistry::default();
        let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let weak = Arc::downgrade(&block);
        let receipt = registry.register(block);
        assert!(weak.upgrade().is_none());
        assert_eq!(registry.0.lock().unwrap().len(), 1);
        drop(receipt);
        assert!(registry.0.lock().unwrap().is_empty());
    }

    #[test]
    fn authorizing_variants_share_a_header_but_not_a_receipt() {
        let registry = ReceiptRegistry::default();
        let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1687107_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let original = registry.register(block.clone());
        let mut receipts = Vec::new();
        let mut orders = std::collections::HashSet::from([original.order]);
        for tag in 0..32 {
            let mut changed = block.clone();
            let coinbase = Arc::make_mut(&mut Arc::make_mut(&mut changed).transactions[0]);
            let zakura_chain::transparent::Input::Coinbase { data, .. } =
                &mut coinbase.inputs_mut()[0]
            else {
                panic!("the first transaction is coinbase");
            };
            data.push(tag);
            assert_eq!(block.hash(), changed.hash());
            assert_eq!(block.transactions[0].hash(), changed.transactions[0].hash());
            assert_ne!(block.auth_data_root(), changed.auth_data_root());
            let receipt = registry.register(changed.clone());
            assert!(orders.insert(receipt.order));
            let duplicate = registry.register(changed);
            assert_eq!(duplicate.order, receipt.order);
            receipts.push((receipt, duplicate));
        }
        drop(receipts);
        assert_eq!(registry.register(block).order, original.order);
        drop(original);
        assert!(registry.0.lock().unwrap().is_empty());
    }

    #[test]
    fn concurrent_identical_bodies_share_receipt_lifetime() {
        let registry = ReceiptRegistry::default();
        let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let barrier = std::sync::Barrier::new(8);
        let receipts = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        registry.register(Arc::new(block.as_ref().clone()))
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        let order = receipts[0].order;
        assert!(receipts.iter().all(|receipt| receipt.order == order));
        drop(receipts);
        assert!(registry.0.lock().unwrap().is_empty());
        assert!(registry.register(block).order > order);
    }

    #[test]
    fn overlapping_receipts_keep_priority_until_the_last_caller_finishes() {
        let registry = ReceiptRegistry::default();
        let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let first = registry.register(block.clone());
        let original_order = first.order;
        let duplicate = registry.register(Arc::new(block.as_ref().clone()));
        assert_eq!(duplicate.order, original_order);

        let mut malformed = block.as_ref().clone();
        malformed.transactions.clear();
        assert_eq!(malformed.hash(), block.hash());
        let malformed = registry.register(Arc::new(malformed));
        assert!(malformed.order > original_order);

        drop((first, malformed));
        assert_eq!(registry.register(block.clone()).order, original_order);
        drop(duplicate);
        assert!(registry.0.lock().unwrap().is_empty());
        assert_eq!(Arc::strong_count(&block), 1);
        assert!(registry.register(block).order > original_order);
    }
}
