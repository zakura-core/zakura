//! Receipt priority for concurrent deliveries of complete blocks.
//!
//! A block's receipt order is normally drawn when its complete body reaches the verifier. A block
//! whose header was admitted first can instead keep the order reserved for that header, provided
//! its body follows within [`body_deadline`]. That stops a small body from overtaking a larger
//! one whose header arrived earlier.
//!
//! Priority belongs to the block, not to a delivery attempt. [`AnnouncementClaims`] holds at most
//! one claim per block hash, never replaces it, and does not consume it when a body reads it. A
//! block announced once and withheld therefore cannot renew its place by being announced again.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

use tokio::time::Instant;
use zakura_chain::{
    block::{self, HeightDiff},
    serialization::ZcashSerialize,
};
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

/// The longest any block keeps its claim while its body is in transit, whatever its size.
///
/// Inside its deadline a withheld body looks the same as a slow honest delivery, so this cap is
/// the most a miner can gain by sitting on an announced block and still winning an equal-work
/// tie. A block padded to about 1.5 MiB gets all of it. The value is an estimate of honest
/// full-block delivery, to be revisited against the `sync.block.announcement_claim.age.seconds`
/// histogram.
pub const ANNOUNCEMENT_PRIORITY_WINDOW: Duration = Duration::from_secs(2);

/// The part of [`body_deadline`] that does not depend on body size: scheduling, the round trip
/// of the body request, and the wait for the verifier.
const CLAIM_BASE_GRACE: Duration = Duration::from_millis(500);

/// The slowest honest body transfer that still keeps a claim, in bytes per second.
const CLAIM_FLOOR_BYTES_PER_SEC: u64 = 1024 * 1024;

/// How far above the verified tip a header can be and still get a claim.
///
/// Only blocks racing at the tip need one. Headers further ahead belong to catch-up, and their
/// bodies cannot arrive inside a deadline anyway.
const MAX_CLAIM_HEIGHT_ABOVE_TIP: HeightDiff = 2;

/// The most claims [`AnnouncementClaims`] holds.
///
/// A full table refuses new claims instead of dropping old ones, so flooding it cannot remove a
/// claim that is already in force. A refused block is ranked by body arrival.
const MAX_ANNOUNCEMENT_CLAIMS: usize = 64;

/// How long after its claim a body of `bytes` serialized bytes still takes the claimed order.
///
/// Transfer time grows with size, so a small block gets less than a full one. Every block gets
/// at most [`ANNOUNCEMENT_PRIORITY_WINDOW`].
fn body_deadline(bytes: usize) -> Duration {
    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    let transfer =
        Duration::from_micros(bytes.saturating_mul(1_000_000) / CLAIM_FLOOR_BYTES_PER_SEC);
    CLAIM_BASE_GRACE
        .saturating_add(transfer)
        .min(ANNOUNCEMENT_PRIORITY_WINDOW)
}

/// A place in receipt order reserved for a block when its header was admitted.
#[derive(Copy, Clone, Debug)]
struct Claim {
    height: block::Height,
    order: u64,
    seen_at: Instant,
}

/// Reserves the next place in receipt order for the block `hash`, whose header arrived at `now`.
fn reserve_claim(hash: block::Hash, height: block::Height, now: Instant) -> (block::Hash, Claim) {
    let claim = Claim {
        height,
        order: next_receipt_order(&NEXT_RECEIPT_ORDER),
        seen_at: now,
    };
    (hash, claim)
}

/// The receipt order of the most recent locally mined block.
///
/// A node does not abandon a block it mined because a rival's header arrived first, so a claim
/// at this height with an earlier order is not honoured while this is set.
#[derive(Debug)]
struct MinedWatermark {
    hash: block::Hash,
    height: block::Height,
    order: u64,
    /// Identifies this watermark to the [`MinedGuard`]s that hold it.
    epoch: u64,
    /// The requests still verifying this block.
    pending: usize,
    /// Whether a request for this block has verified it.
    verified: bool,
}

#[derive(Debug)]
struct ClaimTable {
    /// The highest verified tip height seen. Claims are created only above it and dropped once
    /// they are below it, so a dropped hash can never be claimed again.
    floor: block::Height,
    by_hash: HashMap<block::Hash, Claim>,
    mined: Option<MinedWatermark>,
    /// The epoch given to the next [`MinedWatermark`].
    next_mined_epoch: u64,
}

impl Default for ClaimTable {
    fn default() -> Self {
        Self {
            floor: block::Height::MIN,
            by_hash: HashMap::new(),
            mined: None,
            next_mined_epoch: 0,
        }
    }
}

/// What a body arriving now would find for its block hash.
#[derive(Copy, Clone, Debug)]
struct ClaimView {
    order: u64,
    age: Duration,
    /// A locally mined block at this height outranks the claim.
    voided: bool,
}

/// Receipt-order claims for blocks whose headers were admitted before their bodies arrived.
///
/// Header sync creates claims with [`AnnouncementClaims::begin`], and the block verifier reads
/// them by block hash, so every body transport follows the same rule.
#[derive(Clone, Debug, Default)]
pub struct AnnouncementClaims(Arc<Mutex<ClaimTable>>);

/// Claims reserved for headers that are being applied to the header chain.
///
/// They take effect only with [`PendingClaims::commit`]. Dropping this discards them.
#[derive(Debug)]
#[must_use = "reserved claims take effect only when committed"]
pub struct PendingClaims {
    claims: AnnouncementClaims,
    reserved: Vec<(block::Hash, Claim)>,
}

impl AnnouncementClaims {
    /// Reserves a place in receipt order for each of `headers` that is just above
    /// `verified_tip` and has no claim yet.
    ///
    /// Call this when a header apply starts and commit the result once the header chain has
    /// accepted the headers. The order and time are taken now, so a rival body that arrives
    /// during the apply does not overtake a header that arrived first.
    pub fn begin(
        &self,
        headers: impl IntoIterator<Item = (block::Hash, block::Height)>,
        verified_tip: block::Height,
    ) -> PendingClaims {
        self.begin_at(headers, verified_tip, Instant::now())
    }

    fn begin_at(
        &self,
        headers: impl IntoIterator<Item = (block::Hash, block::Height)>,
        verified_tip: block::Height,
        now: Instant,
    ) -> PendingClaims {
        let claimable: Vec<_> = {
            let mut table = self.table();
            table.floor = table.floor.max(verified_tip);
            let floor = table.floor;
            table.by_hash.retain(|_, claim| claim.height >= floor);
            headers
                .into_iter()
                .filter(|(hash, height)| {
                    (1..=MAX_CLAIM_HEIGHT_ABOVE_TIP).contains(&(*height - floor))
                        && !table.by_hash.contains_key(hash)
                })
                .collect()
        };
        PendingClaims {
            claims: self.clone(),
            reserved: claimable
                .into_iter()
                .map(|(hash, height)| reserve_claim(hash, height, now))
                .collect(),
        }
    }

    /// The claim a body of `hash` arriving at `now` would find. Reading it does not use it up,
    /// so an invalid body under the same header cannot take the claim from the real one.
    fn lookup(&self, hash: block::Hash, now: Instant) -> Option<ClaimView> {
        let table = self.table();
        let claim = table.by_hash.get(&hash)?;
        let voided = table.mined.as_ref().is_some_and(|mined| {
            mined.hash != hash && mined.height == claim.height && claim.order < mined.order
        });
        Some(ClaimView {
            order: claim.order,
            age: now.saturating_duration_since(claim.seen_at),
            voided,
        })
    }

    /// Records that the locally mined block `hash` took `order`, so that a rival's earlier claim
    /// at `height` is not honoured. The record lasts while the returned guard, or any other
    /// guard for the same block, is alive. Once one of them is kept, it stays until a newer
    /// mined block replaces it.
    pub(super) fn mark_mined(
        &self,
        hash: block::Hash,
        height: block::Height,
        order: u64,
    ) -> MinedGuard {
        let mut table = self.table();
        let epoch = match &mut table.mined {
            Some(mined) if mined.hash == hash => {
                mined.pending = mined.pending.saturating_add(1);
                mined.epoch
            }
            _ => {
                let epoch = table.next_mined_epoch;
                table.next_mined_epoch = epoch.wrapping_add(1);
                table.mined = Some(MinedWatermark {
                    hash,
                    height,
                    order,
                    epoch,
                    pending: 1,
                    verified: false,
                });
                epoch
            }
        };
        MinedGuard {
            claims: self.clone(),
            epoch,
        }
    }

    /// Whether a claim is held for `hash`.
    #[cfg(any(test, feature = "proptest-impl"))]
    pub fn is_claimed(&self, hash: block::Hash) -> bool {
        self.table().by_hash.contains_key(&hash)
    }

    fn table(&self) -> MutexGuard<'_, ClaimTable> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl PendingClaims {
    /// Puts the reserved claims in force. A hash that gained a claim in the meantime keeps its
    /// first one, and a full table refuses the rest.
    pub fn commit(self) {
        if self.reserved.is_empty() {
            return;
        }
        let mut refused: u64 = 0;
        {
            let mut table = self.claims.table();
            for (hash, claim) in self.reserved {
                // A claim below the floor would be dropped by the next `begin`.
                if claim.height < table.floor || table.by_hash.contains_key(&hash) {
                    continue;
                }
                if table.by_hash.len() >= MAX_ANNOUNCEMENT_CLAIMS {
                    refused = refused.saturating_add(1);
                    continue;
                }
                table.by_hash.insert(hash, claim);
            }
        }
        if refused > 0 {
            metrics::counter!("sync.block.announcement_claim.refused").increment(refused);
        }
    }
}

/// Keeps a locally mined block's watermark while its verification is active.
///
/// Dropping the last guard for a block removes its watermark, unless one of them was kept.
pub(super) struct MinedGuard {
    claims: AnnouncementClaims,
    epoch: u64,
}

impl MinedGuard {
    /// Keeps the watermark after this guard is gone, because the mined block verified.
    pub(super) fn keep(self) {
        if let Some(mined) = &mut self.claims.table().mined {
            if mined.epoch == self.epoch {
                mined.verified = true;
            }
        }
    }
}

impl Drop for MinedGuard {
    fn drop(&mut self) {
        let mut table = self.claims.table();
        let Some(mined) = &mut table.mined else {
            return;
        };
        // A newer mined block has replaced the watermark this guard was holding.
        if mined.epoch != self.epoch {
            return;
        }
        mined.pending = mined.pending.saturating_sub(1);
        if mined.pending == 0 && !mined.verified {
            table.mined = None;
        }
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
    /// `hash` is `block`'s hash. A body that shares no active receipt takes the order of the
    /// claim held for `hash` in `claims` if it arrived inside the claim's deadline, and the next
    /// order otherwise.
    pub(super) fn register(
        &self,
        hash: block::Hash,
        block: &BlockHandle,
        claims: &AnnouncementClaims,
    ) -> ReceiptGuard {
        self.register_at(hash, block, claims, Instant::now())
    }

    fn register_at(
        &self,
        hash: block::Hash,
        block: &BlockHandle,
        claims: &AnnouncementClaims,
        now: Instant,
    ) -> ReceiptGuard {
        // Sizing a block walks its transactions, so it is done only for a claimed block, and
        // before the registry lock is taken.
        let claim = claims
            .lookup(hash, now)
            .map(|claim| (claim, block.zcash_serialized_size()));
        let new_order = || new_receipt_order(hash, claim);
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

/// The order for a body of `hash` that shares no active receipt.
///
/// `claim` is the claim held for `hash` when the body arrived, with the body's serialized size.
/// This is the only place a claim is resolved.
fn new_receipt_order(hash: block::Hash, claim: Option<(ClaimView, usize)>) -> u64 {
    let (outcome, claimed) = match claim {
        None => ("none", None),
        Some((claim, _)) if claim.voided => ("voided", None),
        Some((claim, bytes)) if claim.age <= body_deadline(bytes) => ("used", Some(claim.order)),
        Some(_) => ("expired", None),
    };
    let source = if claim.is_some() { "header" } else { "none" };
    let size = claim.map_or("unclaimed", |(_, bytes)| size_label(bytes));
    metrics::counter!(
        "sync.block.announcement_claim",
        "outcome" => outcome,
        "source" => source,
        "size" => size,
    )
    .increment(1);
    if let Some((claim, _)) = claim {
        metrics::histogram!(
            "sync.block.announcement_claim.age.seconds",
            "source" => source,
            "size" => size,
        )
        .record(claim.age.as_secs_f64());
    }
    let order = claimed.unwrap_or_else(|| next_receipt_order(&NEXT_RECEIPT_ORDER));
    tracing::trace!(%hash, outcome, order, "resolved the announcement claim for a block body");
    order
}

/// The metric label for a block of `bytes` serialized bytes.
///
/// Transfer time grows with size, so the claim metrics are split by it.
fn size_label(bytes: usize) -> &'static str {
    match bytes {
        0..100_000 => "under_100kB",
        100_000..500_000 => "under_500kB",
        500_000..1_000_000 => "under_1MB",
        _ => "1MB_or_more",
    }
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

    /// Registers `block` the way the verifier does, under its hash, with no claim held.
    fn register(registry: &ReceiptRegistry, block: Arc<Block>) -> ReceiptGuard {
        let block = BlockHandle::from(block);
        registry.register(block.hash(), &block, &AnnouncementClaims::default())
    }

    fn hash(index: usize) -> block::Hash {
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&index.to_le_bytes());
        block::Hash(bytes)
    }

    /// The height every test claim is made at, one above [`TIP`].
    const CLAIMED: block::Height = block::Height(101);
    const TIP: block::Height = block::Height(100);

    /// Puts a claim for `hash` in force at [`CLAIMED`], as header sync does after a header apply.
    fn claim(claims: &AnnouncementClaims, hash: block::Hash, now: Instant) {
        claims.begin_at([(hash, CLAIMED)], TIP, now).commit();
    }

    /// The order a body of `hash` with `bytes` serialized bytes gets when it arrives at `now`.
    fn body_order(
        claims: &AnnouncementClaims,
        hash: block::Hash,
        bytes: usize,
        now: Instant,
    ) -> u64 {
        new_receipt_order(hash, claims.lookup(hash, now).map(|claim| (claim, bytes)))
    }

    fn claimed_order(claims: &AnnouncementClaims, hash: block::Hash) -> Option<u64> {
        claims.lookup(hash, Instant::now()).map(|claim| claim.order)
    }

    #[test]
    fn a_body_inside_its_deadline_keeps_the_order_of_its_header() {
        let claims = AnnouncementClaims::default();
        let now = Instant::now();
        claim(&claims, hash(1), now);
        let competitor = next_receipt_order(&NEXT_RECEIPT_ORDER);

        let at_the_bound = now + body_deadline(0);
        let order = body_order(&claims, hash(1), 0, at_the_bound);
        assert!(order < competitor);
        assert_eq!(Some(order), claimed_order(&claims, hash(1)));
        assert!(
            body_order(&claims, hash(2), 0, now) > competitor,
            "a claim only reserves priority for the hash it was made for",
        );
    }

    #[test]
    fn the_first_claim_for_a_block_is_permanent() {
        let claims = AnnouncementClaims::default();
        let now = Instant::now();
        claim(&claims, hash(1), now);
        let first = claims.lookup(hash(1), now).expect("the block was claimed");

        let later = now + Duration::from_millis(300);
        claim(&claims, hash(1), later);
        let again = claims.lookup(hash(1), later).expect("the claim is kept");
        assert_eq!(again.order, first.order);
        assert_eq!(
            again.age,
            Duration::from_millis(300),
            "the claim still dates from the first header",
        );
    }

    #[test]
    fn announcing_a_withheld_block_again_does_not_renew_its_claim() {
        let claims = AnnouncementClaims::default();
        let now = Instant::now();
        claim(&claims, hash(1), now);

        // The body is withheld past its deadline while the header is delivered again.
        let mut late = now;
        for _ in 0..3 {
            late += ANNOUNCEMENT_PRIORITY_WINDOW;
            claim(&claims, hash(1), late);
        }
        let competitor = next_receipt_order(&NEXT_RECEIPT_ORDER);
        assert!(
            body_order(&claims, hash(1), 2_000_000, late) > competitor,
            "a withheld body is ranked by its arrival",
        );
    }

    #[test]
    fn a_claim_keeps_the_order_reserved_before_its_header_apply() {
        let claims = AnnouncementClaims::default();
        let now = Instant::now();
        let pending = claims.begin_at([(hash(1), CLAIMED)], TIP, now);
        let arrived_during_apply = next_receipt_order(&NEXT_RECEIPT_ORDER);
        assert!(
            claims.lookup(hash(1), now).is_none(),
            "a claim is not in force until its header is applied",
        );

        pending.commit();
        assert!(body_order(&claims, hash(1), 0, now) < arrived_during_apply);
    }

    #[test]
    fn a_header_that_is_not_applied_leaves_no_claim() {
        let claims = AnnouncementClaims::default();
        let now = Instant::now();
        drop(claims.begin_at([(hash(1), CLAIMED)], TIP, now));
        assert!(!claims.is_claimed(hash(1)));

        // A later apply of the same header claims from its own arrival.
        let later = now + ANNOUNCEMENT_PRIORITY_WINDOW;
        claim(&claims, hash(1), later);
        let found = claims.lookup(hash(1), later).expect("the header applied");
        assert_eq!(found.age, Duration::ZERO);
    }

    #[test]
    fn the_deadline_grows_with_body_size_up_to_the_window() {
        const MIB: usize = 1024 * 1024;
        assert_eq!(body_deadline(0), CLAIM_BASE_GRACE);
        assert_eq!(
            body_deadline(MIB),
            CLAIM_BASE_GRACE + Duration::from_secs(1)
        );
        assert_eq!(body_deadline(2 * MIB), ANNOUNCEMENT_PRIORITY_WINDOW);
        assert_eq!(body_deadline(usize::MAX), ANNOUNCEMENT_PRIORITY_WINDOW);

        let claims = AnnouncementClaims::default();
        let now = Instant::now();
        claim(&claims, hash(1), now);
        let competitor = next_receipt_order(&NEXT_RECEIPT_ORDER);

        let past_the_grace = now + CLAIM_BASE_GRACE + Duration::from_millis(1);
        assert!(body_order(&claims, hash(1), 1_000, past_the_grace) > competitor);
        assert!(body_order(&claims, hash(1), MIB, past_the_grace) < competitor);

        let past_the_window = now + ANNOUNCEMENT_PRIORITY_WINDOW + Duration::from_millis(1);
        assert!(body_order(&claims, hash(1), 2 * MIB, past_the_window) > competitor);
    }

    #[test]
    fn only_headers_just_above_the_verified_tip_are_claimed() {
        let claims = AnnouncementClaims::default();
        // A batch that reaches from below the tip to far ahead of it, as catch-up delivers.
        let heights = [99, 100, 101, 102, 103, 5_000].map(block::Height);
        let headers = heights
            .into_iter()
            .enumerate()
            .map(|(index, height)| (hash(index), height));
        claims.begin_at(headers, TIP, Instant::now()).commit();

        let claimed: Vec<_> = heights
            .into_iter()
            .enumerate()
            .filter(|(index, _)| claims.is_claimed(hash(*index)))
            .map(|(_, height)| height)
            .collect();
        assert_eq!(claimed, [block::Height(101), block::Height(102)]);
    }

    #[test]
    fn a_full_table_refuses_new_claims_and_keeps_the_old_ones() {
        let claims = AnnouncementClaims::default();
        let now = Instant::now();
        for index in 0..MAX_ANNOUNCEMENT_CLAIMS {
            claim(&claims, hash(index), now);
        }
        let orders = || -> Vec<_> {
            (0..MAX_ANNOUNCEMENT_CLAIMS)
                .map(|index| claimed_order(&claims, hash(index)).expect("the claim is held"))
                .collect()
        };
        let before = orders();

        claim(&claims, hash(MAX_ANNOUNCEMENT_CLAIMS), now);
        assert!(!claims.is_claimed(hash(MAX_ANNOUNCEMENT_CLAIMS)));
        assert_eq!(orders(), before);
    }

    #[test]
    fn claims_below_the_verified_tip_are_dropped_for_good() {
        let claims = AnnouncementClaims::default();
        let now = Instant::now();
        claim(&claims, hash(1), now);

        // The claimed height is the new tip, where a tie can still be decided.
        claims.begin_at([], CLAIMED, now).commit();
        assert!(claims.is_claimed(hash(1)));

        let above = block::Height(CLAIMED.0 + 1);
        claims.begin_at([], above, now).commit();
        assert!(!claims.is_claimed(hash(1)));

        // Neither a new delivery nor a lower tip brings the claim back.
        claims.begin_at([(hash(1), CLAIMED)], above, now).commit();
        claim(&claims, hash(1), now);
        assert!(!claims.is_claimed(hash(1)));
    }

    #[test]
    fn a_reservation_overtaken_by_the_verified_tip_is_not_committed() {
        let claims = AnnouncementClaims::default();
        let now = Instant::now();
        let pending = claims.begin_at([(hash(1), CLAIMED)], TIP, now);
        claims
            .begin_at([], block::Height(CLAIMED.0 + 1), now)
            .commit();
        pending.commit();
        assert!(!claims.is_claimed(hash(1)));
    }

    #[test]
    fn a_mined_block_outranks_an_earlier_claim_at_its_height() {
        let claims = AnnouncementClaims::default();
        let now = Instant::now();
        claim(&claims, hash(1), now);
        claims
            .begin_at([(hash(2), block::Height(CLAIMED.0 + 1))], TIP, now)
            .commit();
        let rival = claimed_order(&claims, hash(1)).expect("the rival was claimed");
        let above = claimed_order(&claims, hash(2)).expect("the next height was claimed");

        let mined_order = next_receipt_order(&NEXT_RECEIPT_ORDER);
        let mined = claims.mark_mined(hash(3), CLAIMED, mined_order);
        assert!(
            body_order(&claims, hash(1), 0, now) > mined_order,
            "a rival announced first does not displace the mined block",
        );
        assert_eq!(
            body_order(&claims, hash(2), 0, now),
            above,
            "a claim at another height is honoured",
        );

        // The mined block failed verification, so the rival's claim is back in force.
        drop(mined);
        assert_eq!(body_order(&claims, hash(1), 0, now), rival);
    }

    #[test]
    fn a_mined_watermark_lasts_while_any_request_for_the_block_is_active() {
        let claims = AnnouncementClaims::default();
        let now = Instant::now();
        claim(&claims, hash(1), now);
        let rival = claimed_order(&claims, hash(1)).expect("the rival was claimed");
        let voided = || {
            claims
                .lookup(hash(1), now)
                .is_some_and(|claim| claim.voided)
        };

        let mined_order = next_receipt_order(&NEXT_RECEIPT_ORDER);
        let first = claims.mark_mined(hash(3), CLAIMED, mined_order);
        let duplicate = claims.mark_mined(hash(3), CLAIMED, mined_order);
        // The duplicate submission is rejected while the first is still verifying.
        drop(duplicate);
        assert!(voided());

        // The first submission verified, so the watermark outlives its request.
        first.keep();
        assert!(voided());

        // A newer mined block replaces the watermark, and its failure removes it.
        let next = claims.mark_mined(hash(4), block::Height(CLAIMED.0 + 1), mined_order + 1);
        assert!(!voided(), "the watermark names one height");
        drop(next);
        assert_eq!(body_order(&claims, hash(1), 0, now), rival);
    }

    #[test]
    fn a_claim_is_shared_by_every_body_delivered_under_its_header() {
        let registry = ReceiptRegistry::default();
        let claims = AnnouncementClaims::default();
        let now = Instant::now();
        let block: Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        claims
            .begin_at([(block.hash(), block::Height(1))], block::Height(0), now)
            .commit();
        let claimed = claimed_order(&claims, block.hash()).expect("the header was claimed");
        let variant = |count: usize| {
            let mut variant = block.clone();
            for _ in 0..count {
                variant.transactions.push(block.transactions[0].clone());
            }
            assert_eq!(variant.hash(), block.hash());
            BlockHandle::from(Arc::new(variant))
        };

        // Junk bodies fill the tracked slots and spill into untracked receipts. None of them
        // uses the claim up, so the real body still finds it.
        let receipts: Vec<_> = (0..=MAX_BODIES_PER_HASH + 1)
            .map(|count| registry.register_at(block.hash(), &variant(count), &claims, now))
            .collect();
        assert!(receipts[MAX_BODIES_PER_HASH].registered.is_none());
        assert!(receipts.iter().all(|receipt| receipt.order == claimed));

        // A delivery after every earlier one finished is still inside the deadline.
        drop(receipts);
        let again =
            registry.register_at(block.hash(), &variant(0), &claims, now + body_deadline(0));
        assert_eq!(again.order, claimed);
        let late = now + ANNOUNCEMENT_PRIORITY_WINDOW + Duration::from_millis(1);
        drop(again);
        assert!(
            registry
                .register_at(block.hash(), &variant(0), &claims, late)
                .order
                > claimed
        );
    }

    #[test]
    fn a_claimed_body_shares_its_order_with_overlapping_deliveries() {
        let registry = ReceiptRegistry::default();
        let claims = AnnouncementClaims::default();
        let now = Instant::now();
        let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        claims
            .begin_at([(block.hash(), block::Height(1))], block::Height(0), now)
            .commit();
        let handle = BlockHandle::from(block.clone());
        let first = registry.register_at(block.hash(), &handle, &claims, now);
        assert_eq!(Some(first.order), claimed_order(&claims, block.hash()));

        // An overlapping delivery shares the receipt even after the claim's deadline.
        let late = now + ANNOUNCEMENT_PRIORITY_WINDOW + Duration::from_millis(1);
        let duplicate = BlockHandle::from(Arc::new(block.as_ref().clone()));
        assert_eq!(
            registry
                .register_at(block.hash(), &duplicate, &claims, late)
                .order,
            first.order
        );
    }

    #[test]
    fn size_labels_split_at_their_bounds() {
        assert_eq!(size_label(99_999), "under_100kB");
        assert_eq!(size_label(100_000), "under_500kB");
        assert_eq!(size_label(499_999), "under_500kB");
        assert_eq!(size_label(999_999), "under_1MB");
        assert_eq!(size_label(1_000_000), "1MB_or_more");
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
