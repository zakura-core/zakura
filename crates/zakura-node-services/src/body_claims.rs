//! Shared body-ownership claims between v2 block fronts.
//!
//! Body claims live in one hash map behind a non-awaited `Mutex`
//! (`SharedBodyClaims`), so BPv1 and native `block_sync` share them without
//! coupling `block_sync` to BPv1 or changing any legacy path (`dual-1`).
//!
//! Every entry carries an expiry deadline, so a dead entry can never pin a
//! height (`bpv1-57`). An entry is one of two kinds:
//!
//! - A **claim**. BPv1 claims a body before headerchain sees its header, until
//!   its reconstruction deadline, and extends the claim to its verification
//!   deadline once the block assembles (`bpv1-44`, `bpv1-82`).
//! - A **reservation**. Native block sync reserves a body for BPv1 when it
//!   learns a header in BPv1's window before BPv1 claims the body. The
//!   reservation lasts BPv1's metadata grace. BPv1 turns it into a claim when
//!   the block's `HeaderMeta` arrives.
//!
//! Native block sync reserves each hash at most once, so BPv1 delays a body by
//! at most one grace before its claim, and never after a release. Releasing an
//! entry for any reason but [`BodyClaimRelease::Committed`], or installing a
//! claim with an earlier expiry, bumps one watch generation, so native block
//! sync wakes immediately to fetch the missing body or to re-arm its
//! claim-expiry timer.

use std::{
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use zakura_chain::block;
use zakura_rt::{
    collections::{HashMap, HashSet},
    sync::watch,
    time::Instant,
};

/// Reason a body claim is released.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BodyClaimRelease {
    /// Block committed (or delivered as duplicate); native sync does not need to fetch.
    Committed,
    /// BPv1 gave up (deadline, repair exhausted, re-encode fail, or root mismatch);
    /// native block sync should fetch the body immediately.
    Fallback,
    /// Block retired or consensus-invalid.
    Retired,
    /// Every copy of the block's metadata failed its commitment check.
    /// Native block sync fetches the body, and BPv1 never claims it again,
    /// so repeated bad metadata cannot keep deferring native block sync.
    FailedMeta,
}

/// The heights whose bodies BPv1 may fetch, as BPv1 last published them.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Bpv1Window {
    /// BPv1 admits heights strictly above this one (`bpv1-7`).
    pub low_exclusive: u32,
    /// BPv1 admits heights up to and including this one (`bpv1-76`).
    pub high_inclusive: u32,
    /// How long native block sync reserves a body in the window for BPv1.
    pub grace: Duration,
}

impl Bpv1Window {
    fn contains(&self, height: u32) -> bool {
        self.low_exclusive < height && height <= self.high_inclusive
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum EntryKind {
    /// Native block sync holds the body for BPv1's `HeaderMeta`.
    Reserved,
    /// BPv1 owns the body download.
    Claimed,
}

#[derive(Copy, Clone, Debug)]
struct Entry {
    kind: EntryKind,
    expiry: Instant,
}

/// The most hashes that [`Table::seen`] holds. BPv1's window spans
/// `2 × PROPAGATION_DEPTH` heights, so honest forks stay far below it. A
/// peer's statuses can name hashes that never become blocks. At the cap
/// nothing new is reserved, and both syncs fetch at once. A mark that the cap
/// refuses still stops reservations, through [`Table::unmarked_through`].
const MAX_SEEN: usize = 1024;

#[derive(Debug, Default)]
struct Table {
    entries: HashMap<block::Hash, Entry>,
    /// Hashes seen reserved or claimed, or released for a reason other than
    /// commitment, by height. They are never reserved again. A hash leaves
    /// when BPv1's window moves above its height.
    seen: HashMap<block::Hash, u32>,
    /// The highest height of a hash that [`Self::see`] could not record at
    /// the cap. Nothing at or below it is reserved until BPv1's window moves
    /// above it, so a skipped mark never lets a hash be reserved again.
    unmarked_through: Option<u32>,
    /// Hashes released for [`BodyClaimRelease::FailedMeta`], by height. They
    /// are never claimed again, and leave with [`Self::seen`]. Holds at most
    /// [`MAX_SEEN`].
    failed_meta: HashMap<block::Hash, u32>,
    window: Option<Bpv1Window>,
    /// The node ids of the peers with a live BPv1 session. Only they send
    /// this node `HeaderMeta`.
    bpv1_peers: HashSet<[u8; 32]>,
}

impl Table {
    fn prune(&mut self, now: Instant) -> usize {
        let before = self.entries.len();
        self.entries.retain(|_, entry| now < entry.expiry);
        before - self.entries.len()
    }

    /// Records `hash` at `height` in [`Self::seen`], keeping the highest
    /// height seen for it: a header-sync target's height comes from the peer.
    /// At the cap, raises [`Self::unmarked_through`] to `height` instead.
    fn see(&mut self, hash: block::Hash, height: u32) {
        if let Some(marked) = self.seen.get_mut(&hash) {
            *marked = (*marked).max(height);
            return;
        }
        if self.seen.len() < MAX_SEEN {
            self.seen.insert(hash, height);
        } else {
            self.unmarked_through = self.unmarked_through.max(Some(height));
        }
    }

    fn is_live(&self, hash: &block::Hash, now: Instant) -> bool {
        self.entries
            .get(hash)
            .is_some_and(|entry| now < entry.expiry)
    }
}

/// Shared, cheaply cloneable table of body claims and reservations, each
/// keyed by block hash with an expiry deadline.
///
/// The internal `Mutex` is never held across an `.await`.
#[derive(Clone, Debug)]
pub struct SharedBodyClaims {
    table: Arc<Mutex<Table>>,
    /// Bumped on a release for any reason but [`BodyClaimRelease::Committed`],
    /// when a new claim installs an earlier expiry deadline, and when a prune
    /// removes an expired entry. A holder's expiry timer may have been armed
    /// for that entry, so its removal must wake the holder.
    generation_tx: Arc<watch::Sender<u64>>,
}

impl Default for SharedBodyClaims {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedBodyClaims {
    /// Creates a new shared body-claims set.
    pub fn new() -> Self {
        let (generation_tx, _rx) = watch::channel(0u64);
        Self {
            table: Arc::new(Mutex::new(Table::default())),
            generation_tx: Arc::new(generation_tx),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Table> {
        self.table.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Claims `hash` for BPv1 until `deadline`, judging expiry at `now`.
    ///
    /// Returns `true` if the claim is newly installed, replaces an expired
    /// entry, or replaces a live reservation. Returns `false` if a live claim
    /// already exists, `deadline` has passed, or `hash` was released for
    /// [`BodyClaimRelease::FailedMeta`].
    pub fn claim_at(&self, hash: block::Hash, deadline: Instant, now: Instant) -> bool {
        let earlier_expiry = {
            let mut table = self.lock();
            // The comparison includes expired entries, which block sync's
            // timer may still be armed for.
            let earlier = table.entries.values().all(|entry| deadline < entry.expiry);
            let pruned = table.prune(now) > 0;
            let claimed = table
                .entries
                .get(&hash)
                .is_some_and(|entry| entry.kind == EntryKind::Claimed);
            if now >= deadline || claimed || table.failed_meta.contains_key(&hash) {
                drop(table);
                if pruned {
                    self.bump_generation();
                }
                return false;
            }
            table.entries.insert(
                hash,
                Entry {
                    kind: EntryKind::Claimed,
                    expiry: deadline,
                },
            );
            earlier || pruned
        };
        if earlier_expiry {
            self.bump_generation();
        }
        true
    }

    /// Moves the live claim on `hash` to expire at `deadline`, judging expiry
    /// at `now`.
    ///
    /// Returns `true` if a live claim existed and `deadline` is later than its
    /// expiry. A reservation never extends. A later expiry needs no wake:
    /// block sync's timer fires at the earlier one, finds the hash still
    /// claimed, and re-arms.
    pub fn extend_at(&self, hash: block::Hash, deadline: Instant, now: Instant) -> bool {
        let mut table = self.lock();
        match table.entries.get_mut(&hash) {
            Some(entry)
                if entry.kind == EntryKind::Claimed
                    && now < entry.expiry
                    && entry.expiry < deadline =>
            {
                entry.expiry = deadline;
                true
            }
            _ => false,
        }
    }

    /// Releases `hash`. Unless the block committed, notifies subscribers so
    /// native block sync can fetch the body immediately, and marks `hash`
    /// seen, so a later [`Self::defer_to_bpv1`] never reserves it.
    pub fn release(&self, hash: block::Hash, reason: BodyClaimRelease) -> bool {
        let removed = {
            let mut table = self.lock();
            if reason != BodyClaimRelease::Committed {
                // The release does not carry a height. The window's top
                // keeps the mark until the window passes it.
                if let Some(window) = table.window {
                    table.see(hash, window.high_inclusive);
                    if reason == BodyClaimRelease::FailedMeta && table.failed_meta.len() < MAX_SEEN
                    {
                        table.failed_meta.insert(hash, window.high_inclusive);
                    }
                }
            }
            table.entries.remove(&hash).is_some()
        };
        if reason != BodyClaimRelease::Committed {
            self.bump_generation();
        }
        removed
    }

    /// Publishes the heights BPv1 may fetch. `None` turns reservations off.
    ///
    /// Forgets the hashes that native block sync saw at or below the new
    /// window: native block sync never reserves those heights again.
    pub fn set_bpv1_window(&self, window: Option<Bpv1Window>) {
        let mut table = self.lock();
        if let Some(window) = window {
            table
                .seen
                .retain(|_, height| *height > window.low_exclusive);
            table
                .failed_meta
                .retain(|_, height| *height > window.low_exclusive);
            if table
                .unmarked_through
                .is_some_and(|height| height <= window.low_exclusive)
            {
                table.unmarked_through = None;
            }
        }
        table.window = window;
    }

    /// Records whether the peer with node id `peer` has a live BPv1 session.
    /// Without any, no `HeaderMeta` can arrive, so nothing is reserved.
    pub fn set_bpv1_peer(&self, peer: [u8; 32], live: bool) {
        let mut table = self.lock();
        if live {
            table.bpv1_peers.insert(peer);
        } else {
            table.bpv1_peers.remove(&peer);
        }
    }

    /// Whether the peer with node id `peer` has a live BPv1 session, so it
    /// sends this node `HeaderMeta` for the blocks it admits.
    pub fn is_bpv1_peer(&self, peer: &[u8; 32]) -> bool {
        self.lock().bpv1_peers.contains(peer)
    }

    /// Native block sync asks whether to skip the needed body `hash` at
    /// `height` for now, judging expiry at `now`. Header sync asks the same
    /// before it fetches the header `hash` from a BPv1 peer, so the header
    /// and the body share one reservation.
    ///
    /// Returns `true` while a live claim or reservation covers `hash`. If no
    /// entry covers it, and native block sync never saw it covered before,
    /// and `height` lies in BPv1's window while BPv1 is live, reserves it for
    /// the window's grace and returns `true`. Otherwise returns `false`, and
    /// native block sync fetches the body.
    ///
    /// A new reservation does not bump the generation. Each caller re-arms
    /// its own timer from [`Self::next_expiry`].
    pub fn defer_to_bpv1(&self, hash: block::Hash, height: u32, now: Instant) -> bool {
        let mut table = self.lock();
        if table.is_live(&hash, now) {
            table.see(hash, height);
            return true;
        }
        if let Some(marked) = table.seen.get_mut(&hash) {
            *marked = (*marked).max(height);
            return false;
        }
        if table.seen.len() >= MAX_SEEN
            || table
                .unmarked_through
                .is_some_and(|through| height <= through)
            || table.bpv1_peers.is_empty()
        {
            return false;
        }
        let Some(window) = table.window.filter(|window| window.contains(height)) else {
            return false;
        };
        table.entries.insert(
            hash,
            Entry {
                kind: EntryKind::Reserved,
                expiry: now + window.grace,
            },
        );
        table.seen.insert(hash, height);
        true
    }

    /// Runs `transition` only if no hash in `hashes` holds a live claim or
    /// reservation at `now`, and returns its result.
    ///
    /// The claims lock stays held during `transition`, so a concurrent claim
    /// lands either before the check or after the transition. `transition`
    /// must not call back into this table.
    pub fn if_all_unclaimed<'a, T>(
        &self,
        hashes: impl IntoIterator<Item = &'a block::Hash>,
        now: Instant,
        transition: impl FnOnce() -> T,
    ) -> Option<T> {
        let table = self.lock();
        hashes
            .into_iter()
            .all(|hash| !table.is_live(hash, now))
            .then(transition)
    }

    /// Returns `true` iff `hash` currently has an unexpired claim or
    /// reservation.
    pub fn is_claimed(&self, hash: block::Hash, now: Instant) -> bool {
        self.lock().is_live(&hash, now)
    }

    /// Prunes expired entries (`now >= deadline`) and returns how many expired.
    pub fn prune_expired(&self, now: Instant) -> usize {
        let pruned = self.lock().prune(now);
        if pruned > 0 {
            self.bump_generation();
        }
        pruned
    }

    /// Prunes expired entries at `now`, as [`Self::prune_expired`] does, and
    /// returns the earliest remaining expiry, which lies after `now`.
    pub fn next_live_expiry(&self, now: Instant) -> Option<Instant> {
        let (pruned, next) = {
            let mut table = self.lock();
            let pruned = table.prune(now);
            (
                pruned,
                table.entries.values().map(|entry| entry.expiry).min(),
            )
        };
        if pruned > 0 {
            self.bump_generation();
        }
        next
    }

    /// Earliest expiry deadline across entries, for timer wakeups.
    pub fn next_expiry(&self) -> Option<Instant> {
        self.lock().entries.values().map(|entry| entry.expiry).min()
    }

    /// Subscribes to the wake generation. Native block sync re-queries needed
    /// bodies and re-arms its claim-expiry timer on every change.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.generation_tx.subscribe()
    }

    fn bump_generation(&self) {
        self.generation_tx
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn claim_blocks_until_release_or_deadline_expiry() {
        let claims = SharedBodyClaims::new();
        let mut rx = claims.subscribe();
        let t0 = Instant::now();
        let h = block::Hash([0xAA; 32]);

        assert!(claims.claim_at(h, t0 + Duration::from_secs(10), t0));
        assert!(!claims.claim_at(h, t0 + Duration::from_secs(10), t0));
        assert!(claims.is_claimed(h, t0 + Duration::from_secs(5)));
        assert!(!claims.is_claimed(h, t0 + Duration::from_secs(10)));
        // The first claim installed the earliest expiry, which also bumps the generation.
        assert!(rx.has_changed().unwrap());
        rx.mark_unchanged();

        // Releasing with Fallback notifies subscriber immediately.
        assert!(claims.release(h, BodyClaimRelease::Fallback));
        assert!(!claims.is_claimed(h, t0));
        assert!(rx.has_changed().unwrap());
    }

    #[test]
    fn retired_release_wakes_and_committed_release_does_not() {
        let claims = SharedBodyClaims::new();
        let mut rx = claims.subscribe();
        let t0 = Instant::now();
        let (a, b) = (block::Hash([0xAA; 32]), block::Hash([0xBB; 32]));
        assert!(claims.claim_at(a, t0 + Duration::from_secs(10), t0));
        assert!(claims.claim_at(b, t0 + Duration::from_secs(10), t0));
        rx.mark_unchanged();

        assert!(claims.release(a, BodyClaimRelease::Committed));
        assert!(
            !rx.has_changed().unwrap(),
            "a committed body needs no fetch"
        );
        assert!(claims.release(b, BodyClaimRelease::Retired));
        assert!(rx.has_changed().unwrap(), "a retired claim frees the body");
    }

    #[test]
    fn extending_a_live_claim_moves_its_expiry_later_only() {
        let claims = SharedBodyClaims::new();
        let t0 = Instant::now();
        let h = block::Hash([0xAA; 32]);
        let (early, late) = (t0 + Duration::from_secs(10), t0 + Duration::from_secs(30));

        assert!(!claims.extend_at(h, late, t0), "no claim to extend");
        assert!(claims.claim_at(h, early, t0));
        let mut rx = claims.subscribe();
        rx.mark_unchanged();
        assert!(
            !claims.extend_at(h, early, t0),
            "an equal expiry is no extension"
        );
        assert!(claims.extend_at(h, late, t0));
        assert!(!rx.has_changed().unwrap(), "a later expiry wakes nobody");
        assert!(claims.is_claimed(h, early));
        assert_eq!(claims.next_expiry(), Some(late));
        assert!(
            !claims.extend_at(h, early, t0),
            "an extension never shortens"
        );

        assert!(claims.release(h, BodyClaimRelease::Fallback));
        assert!(
            !claims.extend_at(h, late, t0),
            "a released claim stays released"
        );
        assert!(claims.claim_at(h, early, t0));
        assert!(
            !claims.extend_at(h, late, early),
            "an expired claim stays expired"
        );
    }

    #[test]
    fn transition_runs_only_while_every_hash_is_unclaimed() {
        let claims = SharedBodyClaims::new();
        let t0 = Instant::now();
        let (a, b) = (block::Hash([0xAA; 32]), block::Hash([0xBB; 32]));
        assert_eq!(claims.if_all_unclaimed([&a, &b], t0, || 7), Some(7));
        assert!(claims.claim_at(b, t0 + Duration::from_secs(10), t0));
        assert_eq!(claims.if_all_unclaimed([&a, &b], t0, || 7), None);
        assert_eq!(
            claims.if_all_unclaimed([&a, &b], t0 + Duration::from_secs(10), || 7),
            Some(7),
            "an expired claim does not block the transition",
        );
    }

    fn window(low_exclusive: u32, high_inclusive: u32) -> Option<Bpv1Window> {
        Some(Bpv1Window {
            low_exclusive,
            high_inclusive,
            grace: Duration::from_secs(1),
        })
    }

    #[test]
    fn native_reserves_a_body_in_the_window_once() {
        let claims = SharedBodyClaims::new();
        let t0 = Instant::now();
        let h = block::Hash([0xAA; 32]);
        claims.set_bpv1_window(window(84, 116));

        assert!(
            !claims.defer_to_bpv1(h, 101, t0),
            "no live BPv1 session, no reservation"
        );
        assert!(!claims.is_bpv1_peer(&[7; 32]));
        claims.set_bpv1_peer([7; 32], true);
        assert!(
            !claims.defer_to_bpv1(h, 84, t0),
            "the window excludes its low"
        );
        assert!(!claims.defer_to_bpv1(h, 117, t0), "above the window");
        assert!(
            claims.defer_to_bpv1(h, 116, t0),
            "the window includes its high"
        );
        assert!(claims.is_claimed(h, t0));
        assert_eq!(claims.next_expiry(), Some(t0 + Duration::from_secs(1)));
        assert!(claims.defer_to_bpv1(h, 116, t0), "the reservation is live");

        let t1 = t0 + Duration::from_secs(1);
        assert!(!claims.is_claimed(h, t1), "the reservation expires");
        assert_eq!(claims.prune_expired(t1), 1);
        assert!(
            !claims.defer_to_bpv1(h, 116, t1),
            "native block sync reserves each hash once"
        );
    }

    #[test]
    fn bpv1_claim_upgrades_a_reservation_and_a_reservation_never_extends() {
        let claims = SharedBodyClaims::new();
        let t0 = Instant::now();
        let h = block::Hash([0xAA; 32]);
        claims.set_bpv1_window(window(84, 116));
        claims.set_bpv1_peer([7; 32], true);
        assert!(claims.defer_to_bpv1(h, 101, t0));
        let late = t0 + Duration::from_secs(15);
        assert!(
            !claims.extend_at(h, late, t0),
            "a reservation never extends"
        );

        assert!(
            claims.claim_at(h, late, t0),
            "a claim replaces a reservation"
        );
        assert!(
            !claims.claim_at(h, late, t0),
            "a claim does not replace a claim"
        );
        assert!(claims.is_claimed(h, t0 + Duration::from_secs(10)));
        assert!(claims.extend_at(h, late + Duration::from_secs(1), t0));
    }

    #[test]
    fn native_never_reserves_a_body_it_saw_claimed() {
        let claims = SharedBodyClaims::new();
        let t0 = Instant::now();
        let h = block::Hash([0xAA; 32]);
        claims.set_bpv1_window(window(84, 116));
        claims.set_bpv1_peer([7; 32], true);
        assert!(claims.claim_at(h, t0 + Duration::from_secs(15), t0));
        assert!(claims.defer_to_bpv1(h, 101, t0), "the claim is live");

        assert!(claims.release(h, BodyClaimRelease::Fallback));
        assert!(
            !claims.defer_to_bpv1(h, 101, t0),
            "a released body goes to native block sync at once"
        );
    }

    #[test]
    fn a_moving_window_forgets_heights_below_it() {
        let claims = SharedBodyClaims::new();
        let t0 = Instant::now();
        let (a, b) = (block::Hash([0xAA; 32]), block::Hash([0xBB; 32]));
        claims.set_bpv1_window(window(84, 116));
        claims.set_bpv1_peer([7; 32], true);
        assert!(claims.defer_to_bpv1(a, 90, t0));
        assert!(claims.defer_to_bpv1(b, 100, t0));
        let t1 = t0 + Duration::from_secs(1);
        claims.prune_expired(t1);

        claims.set_bpv1_window(window(90, 120));
        assert_eq!(claims.lock().seen.len(), 1, "height 90 left the window");
        assert!(!claims.defer_to_bpv1(b, 100, t1), "height 100 stays seen");

        claims.set_bpv1_window(None);
        let c = block::Hash([0xCC; 32]);
        assert!(
            !claims.defer_to_bpv1(c, 101, t1),
            "no window, no reservation"
        );
    }

    #[test]
    fn any_prune_of_an_expired_entry_wakes_subscribers() {
        let claims = SharedBodyClaims::new();
        let mut generation = claims.subscribe();
        let t0 = Instant::now();
        let (a, b, c) = (
            block::Hash([0xAA; 32]),
            block::Hash([0xBB; 32]),
            block::Hash([0xCC; 32]),
        );
        let t1 = t0 + Duration::from_secs(1);
        assert!(claims.claim_at(a, t1, t0));
        generation.borrow_and_update();

        // Another holder's claim prunes `a`, whose expiry a timer may still wait for.
        assert!(claims.claim_at(b, t1 + Duration::from_secs(9), t1));
        assert!(generation.has_changed().unwrap(), "the claim's prune wakes");
        generation.borrow_and_update();

        assert!(claims.claim_at(c, t1 + Duration::from_secs(1), t1));
        generation.borrow_and_update();
        let t2 = t1 + Duration::from_secs(1);
        assert_eq!(
            claims.next_live_expiry(t2),
            Some(t1 + Duration::from_secs(9)),
            "the earliest expiry after the prune"
        );
        assert!(generation.has_changed().unwrap(), "the read's prune wakes");
        generation.borrow_and_update();
        assert_eq!(claims.prune_expired(t2), 0);
        assert!(
            !generation.has_changed().unwrap(),
            "a prune that removes nothing does not wake"
        );
    }

    #[test]
    fn a_release_before_native_asks_still_hands_the_body_over_at_once() {
        let claims = SharedBodyClaims::new();
        let t0 = Instant::now();
        let (released, committed) = (block::Hash([0xAA; 32]), block::Hash([0xBB; 32]));
        claims.set_bpv1_window(window(84, 116));
        claims.set_bpv1_peer([7; 32], true);
        assert!(claims.claim_at(released, t0 + Duration::from_secs(15), t0));
        assert!(claims.claim_at(committed, t0 + Duration::from_secs(15), t0));

        // Native block sync never asked while the claims were live.
        assert!(claims.release(released, BodyClaimRelease::Fallback));
        assert!(claims.release(committed, BodyClaimRelease::Committed));
        assert!(
            !claims.defer_to_bpv1(released, 101, t0),
            "a fallback release is never reserved afterwards"
        );
        assert!(
            claims.defer_to_bpv1(committed, 101, t0),
            "a committed release marks nothing"
        );

        claims.set_bpv1_window(window(116, 140));
        assert!(
            !claims.lock().seen.contains_key(&released),
            "the mark leaves with the window"
        );
    }

    #[test]
    fn a_full_seen_set_reserves_nothing() {
        let claims = SharedBodyClaims::new();
        let t0 = Instant::now();
        claims.set_bpv1_window(window(84, 116));
        claims.set_bpv1_peer([7; 32], true);
        for index in 0..MAX_SEEN {
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(&u64::try_from(index).unwrap().to_le_bytes());
            assert!(claims.defer_to_bpv1(block::Hash(bytes), 101, t0));
        }
        assert!(
            !claims.defer_to_bpv1(block::Hash([0xFF; 32]), 101, t0),
            "at the cap, both syncs fetch at once"
        );
        assert_eq!(claims.lock().seen.len(), MAX_SEEN);
    }

    #[test]
    fn a_failed_meta_release_refuses_every_later_claim() {
        let claims = SharedBodyClaims::new();
        let t0 = Instant::now();
        let deadline = t0 + Duration::from_secs(15);
        let failed = block::Hash([0xAA; 32]);
        claims.set_bpv1_window(window(84, 116));
        claims.set_bpv1_peer([7; 32], true);
        assert!(claims.claim_at(failed, deadline, t0));
        assert!(claims.release(failed, BodyClaimRelease::FailedMeta));

        assert!(
            !claims.claim_at(failed, deadline, t0),
            "another copy's metadata cannot defer native block sync again"
        );
        assert!(!claims.is_claimed(failed, t0));
        assert!(!claims.defer_to_bpv1(failed, 101, t0));

        claims.set_bpv1_window(window(116, 140));
        assert!(
            claims.claim_at(failed, deadline, t0),
            "the mark leaves with the window"
        );
    }

    #[test]
    fn a_mark_keeps_the_highest_height_it_was_seen_at() {
        let claims = SharedBodyClaims::new();
        let t0 = Instant::now();
        let hash = block::Hash([0xAA; 32]);
        claims.set_bpv1_window(window(84, 116));
        claims.set_bpv1_peer([7; 32], true);
        // Header sync reserves at a peer's height, and native block sync
        // later asks at the header's own height.
        assert!(claims.defer_to_bpv1(hash, 86, t0));
        let expired = t0 + Duration::from_secs(1);
        assert!(!claims.defer_to_bpv1(hash, 101, expired));

        claims.set_bpv1_window(window(90, 122));
        assert!(
            !claims.defer_to_bpv1(hash, 101, expired),
            "the mark stays while the window holds the header's height"
        );
    }

    #[test]
    fn a_release_at_the_cap_is_never_reserved_after_the_window_moves() {
        let claims = SharedBodyClaims::new();
        let t0 = Instant::now();
        let released = block::Hash([0xAA; 32]);
        claims.set_bpv1_window(window(84, 116));
        claims.set_bpv1_peer([7; 32], true);
        assert!(claims.claim_at(released, t0 + Duration::from_secs(15), t0));
        for index in 0..MAX_SEEN {
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(&u64::try_from(index).unwrap().to_le_bytes());
            assert!(claims.defer_to_bpv1(block::Hash(bytes), 85, t0));
        }

        // The full set refuses the release's mark.
        assert!(claims.release(released, BodyClaimRelease::Fallback));
        assert!(!claims.lock().seen.contains_key(&released));

        // The window drops the filler marks but still holds the block.
        claims.set_bpv1_window(window(85, 117));
        assert!(claims.lock().seen.is_empty());
        assert!(
            !claims.defer_to_bpv1(released, 101, t0),
            "a release the cap could not mark is never reserved afterwards"
        );

        claims.set_bpv1_window(window(117, 141));
        assert!(
            claims.defer_to_bpv1(block::Hash([0xBB; 32]), 130, t0),
            "reservations resume once the window passes the skipped mark"
        );
    }
}
