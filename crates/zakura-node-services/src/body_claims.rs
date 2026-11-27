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
use zakura_rt::{collections::HashMap, sync::watch, time::Instant};

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

#[derive(Debug, Default)]
struct Table {
    entries: HashMap<block::Hash, Entry>,
    /// Hashes that native block sync saw reserved or claimed, by height. Native
    /// block sync never reserves them again. A hash leaves when BPv1's window
    /// moves above its height.
    seen: HashMap<block::Hash, u32>,
    window: Option<Bpv1Window>,
    /// Whether BPv1 has a live session to receive `HeaderMeta` from.
    bpv1_live: bool,
}

impl Table {
    fn prune(&mut self, now: Instant) -> usize {
        let before = self.entries.len();
        self.entries.retain(|_, entry| now < entry.expiry);
        before - self.entries.len()
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
    /// and when a new claim installs an earlier expiry deadline.
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
    /// already exists or `deadline` has passed.
    pub fn claim_at(&self, hash: block::Hash, deadline: Instant, now: Instant) -> bool {
        let earlier_expiry = {
            let mut table = self.lock();
            // The comparison includes expired entries, which block sync's
            // timer may still be armed for.
            let earlier = table.entries.values().all(|entry| deadline < entry.expiry);
            table.prune(now);
            let claimed = table
                .entries
                .get(&hash)
                .is_some_and(|entry| entry.kind == EntryKind::Claimed);
            if now >= deadline || claimed {
                return false;
            }
            table.entries.insert(
                hash,
                Entry {
                    kind: EntryKind::Claimed,
                    expiry: deadline,
                },
            );
            earlier
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
    /// native block sync can fetch the body immediately.
    pub fn release(&self, hash: block::Hash, reason: BodyClaimRelease) -> bool {
        let removed = self.lock().entries.remove(&hash).is_some();
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
        }
        table.window = window;
    }

    /// Records whether BPv1 has a live session. Without one, no `HeaderMeta`
    /// can arrive, so native block sync reserves nothing.
    pub fn set_bpv1_live(&self, live: bool) {
        self.lock().bpv1_live = live;
    }

    /// Native block sync asks whether to skip the needed body `hash` at
    /// `height` for now, judging expiry at `now`.
    ///
    /// Returns `true` while a live claim or reservation covers `hash`. If no
    /// entry covers it, and native block sync never saw it covered before,
    /// and `height` lies in BPv1's window while BPv1 is live, reserves it for
    /// the window's grace and returns `true`. Otherwise returns `false`, and
    /// native block sync fetches the body.
    ///
    /// A new reservation does not bump the generation. Native block sync is
    /// the only caller, and re-arms its timer from [`Self::next_expiry`].
    pub fn defer_to_bpv1(&self, hash: block::Hash, height: u32, now: Instant) -> bool {
        let mut table = self.lock();
        if table.is_live(&hash, now) {
            table.seen.entry(hash).or_insert(height);
            return true;
        }
        if table.seen.contains_key(&hash) || !table.bpv1_live {
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
        self.lock().prune(now)
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
        claims.set_bpv1_live(true);
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
        claims.set_bpv1_live(true);
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
        claims.set_bpv1_live(true);
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
        claims.set_bpv1_live(true);
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
}
