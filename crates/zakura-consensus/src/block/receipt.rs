//! Receipt priority for concurrent deliveries and bounded retries of complete blocks.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use zakura_chain::{
    block::{self, Block},
    serialization::ZcashSerialize,
};
use zakura_state::{CommitBlockError, KnownBlock, ValidateContextError};

use super::VerifyBlockError;
use crate::error::{BlockError, TransactionError};

static NEXT_RECEIPT_ORDER: AtomicU64 = AtomicU64::new(1);
const MAX_RETRY_RECEIPTS: usize = 4096;
const MAX_RETRY_RECEIPTS_PER_HASH: usize = 4;
const RETRY_RECEIPT_TTL: Duration = Duration::from_secs(60 * 60);

/// Body-evidence classification protects peer attribution. Receipt retention
/// instead requires missing context or a local failure that can change on retry.
pub(super) fn retains_error(error: &VerifyBlockError) -> bool {
    if let Some(location) = error.duplicate_location() {
        // The overlapping write can still fail after a pending duplicate returns.
        return matches!(location, KnownBlock::Queue | KnownBlock::WriteChannel);
    }
    match error {
        VerifyBlockError::Depth { .. }
        | VerifyBlockError::StateService { .. }
        | VerifyBlockError::ParentUnavailable { .. }
        // A block too far ahead of the local clock can become valid as time passes.
        | VerifyBlockError::Time(_) => true,
        VerifyBlockError::Commit(error) => match error {
            CommitBlockError::ValidateContextError(error) => retains_context_error(error),
            CommitBlockError::HeaderChainError { .. }
            | CommitBlockError::MissingMinedParent
            | CommitBlockError::QueueFull
            | CommitBlockError::WriteTaskExited { .. } => true,
            _ => false,
        },
        VerifyBlockError::Transaction(error)
        | VerifyBlockError::Block {
            source: BlockError::Transaction(error),
        } => retains_transaction_error(error),
        _ => false,
    }
}

/// Keep local context and storage failures, not fixed-parent consensus failures.
fn retains_context_error(error: &ValidateContextError) -> bool {
    matches!(
        error,
        ValidateContextError::MissingSproutTipTree(_)
            | ValidateContextError::NotReadyToBeCommitted { .. }
            | ValidateContextError::VctSuppliedRootUnavailable { .. }
            | ValidateContextError::VctSuppliedRootAwaitingSuccessor { .. }
            | ValidateContextError::VctSproutHandoffRootMismatch { .. }
            | ValidateContextError::NoteCommitmentTreeError(_)
            | ValidateContextError::HistoryTreeError(_)
    )
}

/// Transaction rule failures and generic rejection strings do not earn retries.
fn retains_transaction_error(error: &TransactionError) -> bool {
    match error {
        TransactionError::ValidateContextError(error) => retains_context_error(error),
        TransactionError::TransparentInputNotFound
        | TransactionError::Io(_)
        | TransactionError::InternalDowncastError(_) => true,
        _ => false,
    }
}

/// Active calls keep their bodies. Retries retain only a digest and order, for
/// at most one hour or 4096 entries, so failed input cannot grow memory forever.
#[derive(Clone, Debug, Default)]
pub(super) struct ReceiptRegistry(Arc<Mutex<Registry>>);

#[derive(Debug, Default)]
struct Registry {
    active: HashMap<block::Hash, Vec<Entry>>,
    retries: RetryReceipts,
}

#[derive(Debug)]
struct Entry {
    block: Arc<Block>,
    order: u64,
    callers: usize,
    retry_eligible: bool,
    retryable: bool,
}

impl Registry {
    fn receipt_mut(&mut self, hash: block::Hash, order: u64) -> &mut Entry {
        self.active
            .get_mut(&hash)
            .expect("a live guard has an active block")
            .iter_mut()
            .find(|entry| entry.order == order)
            .expect("a live guard has an active receipt")
    }
}

#[derive(Debug, Default)]
struct RetryReceipts {
    by_hash: HashMap<block::Hash, HashMap<[u8; 32], (Instant, u64)>>,
    by_expiry: BTreeMap<(Instant, u64), (block::Hash, [u8; 32])>,
}

impl RetryReceipts {
    fn prune(&mut self, now: Instant) {
        while let Some((&(expires, _), _)) = self.by_expiry.first_key_value() {
            if expires > now && self.by_expiry.len() <= MAX_RETRY_RECEIPTS {
                break;
            }
            let (_, (hash, body)) = self
                .by_expiry
                .pop_first()
                .expect("the expiry index is nonempty");
            let receipts = self
                .by_hash
                .get_mut(&hash)
                .expect("expiry entries have a receipt");
            receipts.remove(&body);
            if receipts.is_empty() {
                self.by_hash.remove(&hash);
            }
        }
    }

    fn take(&mut self, hash: block::Hash, block: &Block) -> Option<u64> {
        let receipts = self.by_hash.get_mut(&hash)?;
        let entry = receipts.remove(&body_digest(block))?;
        self.by_expiry.remove(&entry);
        if receipts.is_empty() {
            self.by_hash.remove(&hash);
        }
        Some(entry.1)
    }

    fn insert(&mut self, hash: block::Hash, block: &Block, order: u64, now: Instant) {
        let body = body_digest(block);
        let entry = (now + RETRY_RECEIPT_TTL, order);
        let receipts = self.by_hash.entry(hash).or_default();
        if let Some(previous) = receipts.insert(body, entry) {
            self.by_expiry.remove(&previous);
        }
        self.by_expiry.insert(entry, (hash, body));
        // One checked header can accompany many unchecked bodies, including
        // canceled attempts. Evict its own variants before unrelated receipts.
        if receipts.len() > MAX_RETRY_RECEIPTS_PER_HASH {
            let (&old_body, &old_entry) = receipts
                .iter()
                .min_by_key(|(_, entry)| **entry)
                .expect("a header exceeding its receipt limit has entries");
            receipts.remove(&old_body);
            self.by_expiry.remove(&old_entry);
        }
        self.prune(now);
    }
}

fn body_digest(block: &Block) -> [u8; 32] {
    let mut digest = blake2b_simd::Params::new().hash_length(32).to_state();
    block
        .zcash_serialize(&mut digest)
        .expect("serializing a parsed block to a hash cannot fail");
    digest
        .finalize()
        .as_bytes()
        .try_into()
        .expect("the digest is configured for 32 bytes")
}

/// Keeps the earliest receipt through overlapping calls and retryable failures.
pub(super) struct ReceiptGuard {
    registry: Arc<Mutex<Registry>>,
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
        registry.retries.prune(Instant::now());
        // Different unvalidated bodies with the same header must not share priority.
        let existing = registry
            .active
            .get_mut(&hash)
            .and_then(|entries| entries.iter_mut().find(|entry| entry.block == block));
        let order = if let Some(entry) = existing {
            entry.callers = entry
                .callers
                .checked_add(1)
                .expect("each active caller owns memory, so its count fits in usize");
            entry.order
        } else {
            let saved_order = registry.retries.take(hash, &block);
            let order = saved_order.unwrap_or_else(|| {
                NEXT_RECEIPT_ORDER
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |order| {
                        order.checked_add(1)
                    })
                    .expect("a process cannot receive u64::MAX blocks")
            });
            registry.active.entry(hash).or_default().push(Entry {
                block,
                order,
                callers: 1,
                retry_eligible: saved_order.is_some(),
                retryable: true,
            });
            order
        };
        ReceiptGuard {
            registry: self.0.clone(),
            hash,
            order,
        }
    }

    /// Only checked proof of work (or the network's authenticated waiver) earns
    /// cache space. Cancellation before that point must not retain unchecked input.
    pub(super) fn allow_retry(&self, hash: block::Hash, order: u64) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .receipt_mut(hash, order)
            .retry_eligible = true;
    }
}

impl ReceiptGuard {
    /// Success, committed duplicates, and invalid bodies no longer need a retry receipt.
    pub(super) fn finish(self, retryable: bool) {
        if !retryable {
            let mut registry = self
                .registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            registry.receipt_mut(self.hash, self.order).retryable = false;
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
            .active
            .get_mut(&self.hash)
            .expect("a live guard has a registered block");
        let index = entries
            .iter()
            .position(|entry| entry.order == self.order)
            .expect("a live guard has a registered receipt");
        entries[index].callers -= 1;
        if entries[index].callers == 0 {
            let entry = entries.swap_remove(index);
            if entries.is_empty() {
                registry.active.remove(&self.hash);
            }
            if entry.retry_eligible && entry.retryable {
                registry
                    .retries
                    .insert(self.hash, &entry.block, entry.order, Instant::now());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::serialization::ZcashDeserializeInto;

    fn checked_receipt(registry: &ReceiptRegistry, block: Arc<Block>) -> ReceiptGuard {
        let receipt = registry.register(block.clone());
        registry.allow_retry(block.hash(), receipt.order);
        receipt
    }

    #[test]
    fn retries_and_overlapping_receipts_keep_priority_without_retaining_bodies() {
        let registry = ReceiptRegistry::default();
        let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let first = checked_receipt(&registry, block.clone());
        let original_order = first.order;
        let duplicate = registry.register(Arc::new(block.as_ref().clone()));
        assert_eq!(duplicate.order, original_order);
        drop((first, duplicate));
        assert!(registry.0.lock().unwrap().active.is_empty());
        assert_eq!(Arc::strong_count(&block), 1);

        let mut malformed = block.as_ref().clone();
        malformed.transactions.clear();
        assert_eq!(malformed.hash(), block.hash());
        let malformed = registry.register(Arc::new(malformed));
        assert!(malformed.order > original_order);
        malformed.finish(false);
        let retry = registry.register(block.clone());
        assert_eq!(retry.order, original_order);
        let overlap = registry.register(block.clone());
        retry.finish(false);
        drop(overlap);
        assert!(registry.0.lock().unwrap().retries.by_expiry.is_empty());
        assert!(registry.register(block).order > original_order);
    }

    #[test]
    fn retry_receipts_expire_and_evict_at_the_capacity_bound() {
        let registry = ReceiptRegistry::default();
        let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let first = checked_receipt(&registry, block.clone());
        let original_order = first.order;
        drop(first);
        registry
            .0
            .lock()
            .unwrap()
            .retries
            .prune(Instant::now() + RETRY_RECEIPT_TTL);
        let retry = checked_receipt(&registry, block.clone());
        assert!(retry.order > original_order);
        let retry_order = retry.order;
        drop(retry);
        for nonce in 0..MAX_RETRY_RECEIPTS {
            let mut next = block.as_ref().clone();
            Arc::make_mut(&mut next.header).nonce.0[..8]
                .copy_from_slice(&u64::try_from(nonce).unwrap().to_le_bytes());
            drop(checked_receipt(&registry, Arc::new(next)));
        }
        assert_eq!(
            registry.0.lock().unwrap().retries.by_expiry.len(),
            MAX_RETRY_RECEIPTS
        );
        assert!(registry.register(block).order > retry_order);
    }

    #[test]
    fn retrying_and_canceling_body_variants_cannot_evict_unrelated_receipts() {
        let registry = ReceiptRegistry::default();
        let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let first = checked_receipt(&registry, block.clone());
        let original_order = first.order;
        first.finish(true);

        // Leave exactly one header's allowance available in the global cache.
        for nonce in 0..MAX_RETRY_RECEIPTS - MAX_RETRY_RECEIPTS_PER_HASH - 1 {
            let mut other = block.as_ref().clone();
            Arc::make_mut(&mut other.header).nonce.0[..8]
                .copy_from_slice(&u64::try_from(nonce).unwrap().to_le_bytes());
            checked_receipt(&registry, Arc::new(other)).finish(true);
        }

        let mut variant = block.as_ref().clone();
        Arc::make_mut(&mut variant.header).nonce.0[8] ^= 1;
        let variant_hash = variant.hash();
        let mut last_variant = None;
        for value in 0..MAX_RETRY_RECEIPTS * 2 {
            Arc::make_mut(&mut variant.transactions[0]).outputs_mut()[0].value =
                u64::try_from(value).unwrap().try_into().unwrap();
            assert_eq!(variant.hash(), variant_hash);
            let body = Arc::new(variant.clone());
            let receipt = checked_receipt(&registry, body.clone());
            last_variant = Some((body, receipt.order));
            if value % 2 == 0 {
                receipt.finish(true);
            } else {
                // Cancellation drops the guard without classifying the body.
                drop(receipt);
            }
        }

        {
            let registry = registry.0.lock().unwrap();
            assert_eq!(registry.retries.by_expiry.len(), MAX_RETRY_RECEIPTS);
            assert_eq!(
                registry.retries.by_hash[&variant_hash].len(),
                MAX_RETRY_RECEIPTS_PER_HASH
            );
            assert!(registry.active.is_empty());
        }
        assert_eq!(registry.register(block).order, original_order);
        let (last_body, last_order) = last_variant.unwrap();
        assert_eq!(registry.register(last_body).order, last_order);
    }
}
