//! Shared body-ownership claims between v2 block fronts.
//!
//! Body claims live in one hash map behind a non-awaited `Mutex`
//! (`SharedBodyClaims`), so BPv1 and native `block_sync` share them without
//! coupling `block_sync` to BPv1 or changing any legacy path (`dual-1`).
//!
//! Every claim carries an expiry deadline, so a dead claim can never pin a
//! height (`bpv1-57`). BPv1 claims a body until its reconstruction deadline and
//! extends the claim to its verification deadline once the block assembles
//! (`bpv1-44`, `bpv1-82`). Releasing a claim for any
//! reason but [`BodyClaimRelease::Committed`], or installing a claim with an
//! earlier expiry, bumps one watch generation, so native block sync wakes
//! immediately to fetch the missing body or to re-arm its claim-expiry timer.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

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

/// Shared, cheaply cloneable table of body claims: each claimed block hash
/// maps to its expiry deadline.
///
/// The internal `Mutex` is never held across an `.await`.
#[derive(Clone, Debug)]
pub struct SharedBodyClaims {
    claims: Arc<Mutex<HashMap<block::Hash, Instant>>>,
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
            claims: Arc::new(Mutex::new(HashMap::new())),
            generation_tx: Arc::new(generation_tx),
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<block::Hash, Instant>> {
        self.claims.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Claims `hash` until `deadline`, judging expiry at `now`.
    ///
    /// Returns `true` if the claim is newly installed (or replaces an expired
    /// one), `false` if an unexpired claim already exists or `deadline` has
    /// passed.
    pub fn claim_at(&self, hash: block::Hash, deadline: Instant, now: Instant) -> bool {
        let earlier_expiry = {
            let mut claims = self.lock();
            // The comparison includes expired claims, which block sync's timer
            // may still be armed for.
            let earlier = claims.values().all(|&expiry| deadline < expiry);
            claims.retain(|_, expiry| now < *expiry);
            if now >= deadline || claims.contains_key(&hash) {
                return false;
            }
            claims.insert(hash, deadline);
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
    /// expiry. A later expiry needs no wake: block sync's timer fires at the
    /// earlier one, finds the hash still claimed, and re-arms.
    pub fn extend_at(&self, hash: block::Hash, deadline: Instant, now: Instant) -> bool {
        let mut claims = self.lock();
        match claims.get_mut(&hash) {
            Some(expiry) if now < *expiry && *expiry < deadline => {
                *expiry = deadline;
                true
            }
            _ => false,
        }
    }

    /// Releases `hash`. Unless the block committed, notifies subscribers so
    /// native block sync can fetch the body immediately.
    pub fn release(&self, hash: block::Hash, reason: BodyClaimRelease) -> bool {
        let removed = self.lock().remove(&hash).is_some();
        if reason != BodyClaimRelease::Committed {
            self.bump_generation();
        }
        removed
    }

    /// Runs `transition` only if no hash in `hashes` holds a live claim at
    /// `now`, and returns its result.
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
        let claims = self.lock();
        hashes
            .into_iter()
            .all(|hash| claims.get(hash).is_none_or(|&expiry| now >= expiry))
            .then(transition)
    }

    /// Returns `true` iff `hash` currently has an unexpired claim.
    pub fn is_claimed(&self, hash: block::Hash, now: Instant) -> bool {
        self.lock().get(&hash).is_some_and(|&expiry| now < expiry)
    }

    /// Prunes expired claims (`now >= deadline`) and returns how many expired.
    pub fn prune_expired(&self, now: Instant) -> usize {
        let mut claims = self.lock();
        let before = claims.len();
        claims.retain(|_, expiry| now < *expiry);
        before - claims.len()
    }

    /// Earliest expiry deadline across active claims, for timer wakeups.
    pub fn next_expiry(&self) -> Option<Instant> {
        self.lock().values().min().copied()
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
}
