//! Receipt priority for concurrent deliveries of complete blocks.
//!
//! A block's receipt order is normally drawn when its complete body reaches the verifier. A block
//! that was announced first, by hash or by a header with valid proof of work, can instead keep
//! the order drawn at its announcement, provided its body follows within
//! [`ANNOUNCEMENT_PRIORITY_WINDOW`]. That stops a small body from overtaking a larger one that
//! was announced earlier.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::time::Instant;
use zakura_chain::block;
use zakura_node_services::block_memory::BlockHandle;

static NEXT_RECEIPT_ORDER: AtomicU64 = AtomicU64::new(1);

fn next_receipt_order(counter: &AtomicU64) -> u64 {
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

/// How long an announced block keeps its announcement's place in receipt order while its body is
/// still in transit.
///
/// Inside this window a withheld body looks the same as a slow honest delivery, so the window is
/// both the transfer delay forgiven for large blocks and the longest a miner can sit on an
/// announced block and still win an equal-work tie. Without a bound, announcing and withholding
/// would win every tie. The value is an estimate of honest full-block delivery, to be revisited
/// against the `sync.block.announcement_claim.age.seconds` histogram.
pub const ANNOUNCEMENT_PRIORITY_WINDOW: Duration = Duration::from_secs(2);

/// The most announced blocks [`AnnouncementClaims`] remembers while their bodies are in transit.
///
/// Only blocks racing at the tip need a claim, and a claim is useless once it is older than
/// [`ANNOUNCEMENT_PRIORITY_WINDOW`], so the oldest is dropped first.
const MAX_ANNOUNCEMENT_CLAIMS: usize = 64;

/// A place in receipt order reserved when a block was first announced.
///
/// The block hash commits to the header and its proof of work, so a claim cannot be made for a
/// block that does not exist yet, and it only takes effect once a complete block with that hash
/// reaches the verifier inside [`ANNOUNCEMENT_PRIORITY_WINDOW`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiptClaim {
    hash: block::Hash,
    order: u64,
    claimed_at: Instant,
}

impl ReceiptClaim {
    /// Reserves the next place in receipt order for the block announced as `hash`.
    pub fn announce(hash: block::Hash) -> Self {
        Self {
            hash,
            order: next_receipt_order(&NEXT_RECEIPT_ORDER),
            claimed_at: Instant::now(),
        }
    }

    /// The reserved order, if this claim is for `hash` and its body arrived at `now`, inside the
    /// window.
    fn order_at(&self, hash: block::Hash, now: Instant) -> Option<u64> {
        (self.hash == hash && self.age_at(now) <= ANNOUNCEMENT_PRIORITY_WINDOW)
            .then_some(self.order)
    }

    fn age_at(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.claimed_at)
    }
}

/// Claims for announced blocks whose bodies are fetched by a different task than the one that
/// saw the announcement.
#[derive(Clone, Debug, Default)]
pub struct AnnouncementClaims(Arc<Mutex<VecDeque<ReceiptClaim>>>);

impl AnnouncementClaims {
    /// Records the first announcement of `hash`. Later announcements keep the first claim.
    pub fn announce(&self, hash: block::Hash) {
        let mut claims = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if claims.iter().any(|claim| claim.hash == hash) {
            return;
        }
        if claims.len() >= MAX_ANNOUNCEMENT_CLAIMS {
            claims.pop_front();
        }
        claims.push_back(ReceiptClaim::announce(hash));
    }

    /// Removes and returns the claim for `hash`, once its body is ready for the verifier.
    pub fn take(&self, hash: block::Hash) -> Option<ReceiptClaim> {
        let mut claims = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let index = claims.iter().position(|claim| claim.hash == hash)?;
        claims.remove(index)
    }
}

/// The most distinct bodies tracked under one block hash at a time.
///
/// A peer can deliver any number of bodies under a real header, all but one invalid, and each
/// delivery is compared with the bodies already tracked under that hash. Past this bound a body
/// gets a receipt of its own without being tracked, so an overlapping duplicate of it can only
/// receive a later receipt.
const MAX_BODIES_PER_HASH: usize = 4;

/// Overlapping deliveries of the same complete block share its first receipt.
/// Entries live only while a verification of that body is active, so a block
/// delivered again after every attempt finishes or is cancelled gets a new receipt.
///
/// Deliveries are matched by block hash, then by body equality. Receipts are taken before the
/// header's commitment to the transactions' authorizing data is checked, so bodies that differ
/// only in that data share a block hash but not a receipt.
#[derive(Clone, Debug, Default)]
pub(super) struct ReceiptRegistry(Arc<Mutex<HashMap<block::Hash, Vec<Entry>>>>);

#[derive(Debug)]
struct Entry {
    /// The delivered body, held while a verification of it is active. The delivering
    /// verification holds the same block, and its decoded-memory charge, for at least as long.
    block: BlockHandle,
    order: u64,
    callers: usize,
}

/// Keeps a block's receipt active until its verification finishes or is cancelled.
pub(super) struct ReceiptGuard {
    registry: Arc<Mutex<HashMap<block::Hash, Vec<Entry>>>>,
    /// The hash and body this receipt is registered under, or `None` for an untracked receipt.
    registered: Option<(block::Hash, BlockHandle)>,
    pub(super) order: u64,
}

impl ReceiptRegistry {
    /// Called synchronously by the verifier before returning its request future.
    ///
    /// `hash` is `block`'s hash. A body tracked for the first time takes `claim`'s order if the
    /// claim is still inside its window, and the next order otherwise.
    pub(super) fn register(
        &self,
        hash: block::Hash,
        block: &BlockHandle,
        claim: Option<&ReceiptClaim>,
    ) -> ReceiptGuard {
        let new_order = || new_receipt_order(hash, claim, Instant::now());
        let mut active = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let bodies = active.entry(hash).or_default();
        // Handle equality compares the blocks, skipping headers and transactions they share.
        let (order, registered) =
            if let Some(entry) = bodies.iter_mut().find(|entry| entry.block == *block) {
                entry.callers = entry
                    .callers
                    .checked_add(1)
                    .expect("each active caller owns memory, so its count fits in usize");
                (entry.order, Some((hash, entry.block.clone())))
            } else if bodies.len() < MAX_BODIES_PER_HASH {
                let order = new_order();
                bodies.push(Entry {
                    block: block.clone(),
                    order,
                    callers: 1,
                });
                (order, Some((hash, block.clone())))
            } else {
                (new_order(), None)
            };
        ReceiptGuard {
            registry: self.0.clone(),
            registered,
            order,
        }
    }
}

/// The order for a body received at `now` that shares no active receipt.
fn new_receipt_order(hash: block::Hash, claim: Option<&ReceiptClaim>, now: Instant) -> u64 {
    let claimed = claim.and_then(|claim| claim.order_at(hash, now));
    let outcome = match (claim, claimed) {
        (None, _) => "none",
        (Some(_), Some(_)) => "used",
        (Some(_), None) => "expired",
    };
    metrics::counter!("sync.block.announcement_claim", "outcome" => outcome).increment(1);
    if let Some(claim) = claim {
        metrics::histogram!("sync.block.announcement_claim.age.seconds")
            .record(claim.age_at(now).as_secs_f64());
    }
    claimed.unwrap_or_else(|| next_receipt_order(&NEXT_RECEIPT_ORDER))
}

impl Drop for ReceiptGuard {
    fn drop(&mut self) {
        let Some((hash, block)) = &self.registered else {
            return;
        };
        let mut active = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let bodies = active
            .get_mut(hash)
            .expect("a live guard has a registered receipt");
        let index = bodies
            .iter()
            .position(|entry| std::ptr::eq(entry.block.view(), block.view()))
            .expect("a live guard holds the body its receipt is registered under");
        bodies[index].callers -= 1;
        // This guard still holds the body, so removing the entry does not free it under the lock.
        if bodies[index].callers == 0 {
            bodies.swap_remove(index);
        }
        if bodies.is_empty() {
            active.remove(hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};

    /// Registers `block` the way the verifier does, under its hash.
    fn register(registry: &ReceiptRegistry, block: Arc<Block>) -> ReceiptGuard {
        let block = BlockHandle::from(block);
        registry.register(block.hash(), &block, None)
    }

    #[test]
    fn a_claim_inside_the_window_keeps_its_announcement_order() {
        let hash = block::Hash([1; 32]);
        let claim = ReceiptClaim::announce(hash);
        let later_body = next_receipt_order(&NEXT_RECEIPT_ORDER);
        assert!(claim.order < later_body);

        let at_the_bound = claim.claimed_at + ANNOUNCEMENT_PRIORITY_WINDOW;
        assert_eq!(
            new_receipt_order(hash, Some(&claim), at_the_bound),
            claim.order
        );
    }

    #[test]
    fn an_expired_or_mismatched_claim_takes_the_next_order() {
        let hash = block::Hash([2; 32]);
        let claim = ReceiptClaim::announce(hash);
        let competitor = next_receipt_order(&NEXT_RECEIPT_ORDER);

        let past_the_bound =
            claim.claimed_at + ANNOUNCEMENT_PRIORITY_WINDOW + Duration::from_millis(1);
        assert!(new_receipt_order(hash, Some(&claim), past_the_bound) > competitor);
        assert!(
            new_receipt_order(block::Hash([3; 32]), Some(&claim), claim.claimed_at) > competitor,
            "a claim only reserves priority for the hash it announced",
        );
    }

    #[test]
    fn a_claimed_body_shares_its_order_with_overlapping_deliveries() {
        let registry = ReceiptRegistry::default();
        let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let claim = ReceiptClaim::announce(block.hash());
        let handle = BlockHandle::from(block.clone());
        let claimed = registry.register(block.hash(), &handle, Some(&claim));
        assert_eq!(claimed.order, claim.order);
        assert_eq!(register(&registry, block).order, claim.order);
    }

    #[test]
    fn announcement_claims_keep_the_first_claim_and_drop_the_oldest() {
        let claims = AnnouncementClaims::default();
        let hash = |index: usize| {
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(&index.to_le_bytes());
            block::Hash(bytes)
        };

        claims.announce(hash(1));
        claims.announce(hash(1));
        let first = claims.take(hash(1)).expect("the block was announced");
        assert!(claims.take(hash(1)).is_none(), "a claim is taken once");

        for index in 0..=MAX_ANNOUNCEMENT_CLAIMS {
            claims.announce(hash(index));
        }
        assert!(
            claims.take(hash(0)).is_none(),
            "the oldest claim is dropped"
        );
        let newest = claims
            .take(hash(MAX_ANNOUNCEMENT_CLAIMS))
            .expect("the newest claim is kept");
        assert!(first.order < newest.order);
    }

    #[test]
    fn receipt_order_never_wraps_on_exhaustion() {
        let counter = AtomicU64::new(u64::MAX - 1);
        assert_eq!(next_receipt_order(&counter), u64::MAX - 1);
        assert!(std::panic::catch_unwind(|| next_receipt_order(&counter)).is_err());
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
                            .map(|_| next_receipt_order(&counter))
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

    #[test]
    fn receipts_release_bodies_when_the_last_caller_finishes() {
        let registry = ReceiptRegistry::default();
        let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let weak = Arc::downgrade(&block);
        let receipt = register(&registry, block);
        assert!(weak.upgrade().is_some());
        assert_eq!(registry.0.lock().unwrap().len(), 1);
        drop(receipt);
        assert!(weak.upgrade().is_none());
        assert!(registry.0.lock().unwrap().is_empty());
    }

    #[test]
    fn bodies_past_the_per_hash_bound_get_untracked_receipts() {
        let registry = ReceiptRegistry::default();
        let block: Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let variant = |count: usize| {
            let mut variant = block.clone();
            variant.transactions.truncate(1);
            for _ in 0..count {
                variant.transactions.push(block.transactions[0].clone());
            }
            assert_eq!(variant.hash(), block.hash());
            Arc::new(variant)
        };

        let tracked: Vec<_> = (0..MAX_BODIES_PER_HASH)
            .map(|count| register(&registry, variant(count)))
            .collect();
        let untracked = register(&registry, variant(MAX_BODIES_PER_HASH));
        let untracked_again = register(&registry, variant(MAX_BODIES_PER_HASH));
        assert!(untracked.registered.is_none());
        assert!(tracked
            .iter()
            .all(|receipt| receipt.order < untracked.order));
        assert!(untracked.order < untracked_again.order);
        assert_eq!(
            register(&registry, variant(0)).order,
            tracked[0].order,
            "a tracked body still shares its receipt past the bound",
        );
        assert_eq!(
            registry.0.lock().unwrap()[&block.hash()].len(),
            MAX_BODIES_PER_HASH
        );

        drop((tracked, untracked, untracked_again));
        assert!(registry.0.lock().unwrap().is_empty());
    }

    #[test]
    fn authorizing_variants_share_a_header_but_not_a_receipt() {
        let registry = ReceiptRegistry::default();
        let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1687107_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let original = register(&registry, block.clone());
        let mut receipts = Vec::new();
        let mut orders = std::collections::HashSet::from([original.order]);
        // The original and these variants fill the per-hash bound, so every body is tracked.
        let variants = u8::try_from(MAX_BODIES_PER_HASH - 1).expect("the bound is small");
        for tag in 0..variants {
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
            let receipt = register(&registry, changed.clone());
            assert!(orders.insert(receipt.order));
            let duplicate = register(&registry, changed);
            assert_eq!(duplicate.order, receipt.order);
            receipts.push((receipt, duplicate));
        }
        drop(receipts);
        assert_eq!(register(&registry, block).order, original.order);
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
                        register(&registry, Arc::new(block.as_ref().clone()))
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
        assert!(register(&registry, block).order > order);
    }

    #[test]
    fn overlapping_receipts_keep_priority_until_the_last_caller_finishes() {
        let registry = ReceiptRegistry::default();
        let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let first = register(&registry, block.clone());
        let original_order = first.order;
        let duplicate = register(&registry, Arc::new(block.as_ref().clone()));
        assert_eq!(duplicate.order, original_order);

        let mut malformed = block.as_ref().clone();
        malformed.transactions.clear();
        assert_eq!(malformed.hash(), block.hash());
        let malformed = register(&registry, Arc::new(malformed));
        assert!(malformed.order > original_order);

        drop((first, malformed));
        assert_eq!(register(&registry, block.clone()).order, original_order);
        drop(duplicate);
        assert!(registry.0.lock().unwrap().is_empty());
        assert_eq!(Arc::strong_count(&block), 1);
        assert!(register(&registry, block).order > original_order);
    }
}
