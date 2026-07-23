//! Branch-owned body-availability retry episodes.

use std::collections::{BTreeSet, HashMap};

use chrono::{DateTime, Duration, Utc};
use zakura_header_chain::{
    BodyUnavailableSummary, BranchId, Clock, Frontier, HeaderGeneration, SourceId,
};

const ALARM_ATTEMPTS: u32 = 10;
const ALARM_AFTER: Duration = Duration::minutes(10);
const ALARM_PROBE_INTERVAL: Duration = Duration::minutes(10);
const MAX_BACKOFF_SECONDS: i64 = 60;
const MAX_JITTER_PER_THOUSAND: i16 = 100;

/// Injected deterministic jitter, bounded by the scheduler to plus or minus ten percent.
pub trait RetryJitter {
    /// Return signed per-thousand jitter for one stable retry identity and attempt.
    fn offset_per_thousand(&self, branch: BranchId, header: Frontier, attempt: u32) -> i16;
}

/// Stable seeded jitter derived from the exact branch, header, and attempt identity.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct SeededRetryJitter {
    seed: [u8; 32],
}

impl SeededRetryJitter {
    /// Construct deterministic node-local jitter from an authenticated or random seed.
    pub const fn new(seed: [u8; 32]) -> Self {
        Self { seed }
    }
}

impl RetryJitter for SeededRetryJitter {
    fn offset_per_thousand(&self, branch: BranchId, header: Frontier, attempt: u32) -> i16 {
        let mut state = blake2b_simd::Params::new()
            .hash_length(2)
            .personal(b"ZkBodyRetryV1__")
            .to_state();
        state.update(&self.seed);
        state.update(&branch.anchor_hash.0);
        state.update(&branch.target_tip_hash.0);
        state.update(&header.height.0.to_le_bytes());
        state.update(&header.hash.0);
        state.update(&attempt.to_le_bytes());
        let digest = state.finalize();
        let sample = u16::from_le_bytes(
            digest.as_bytes()[..2]
                .try_into()
                .expect("the configured digest contains exactly two bytes"),
        );
        i16::try_from(sample % 201).expect("a value modulo 201 fits in i16") - 100
    }
}

/// Result of recording one supplier failure.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RetryUpdate {
    /// A repeat failure arrived before that supplier was eligible to retry.
    TooEarly,
    /// Retry remains active at the returned time.
    RetryAt(DateTime<Utc>),
    /// This failure made persistent unavailability visible.
    Alarmed {
        /// Earliest persistent-alarm probe time.
        probe_at: DateTime<Utc>,
    },
    /// An already-alarmed probe failed and the next bounded probe was scheduled.
    ProbeAt(DateTime<Utc>),
}

/// One selected-branch body-unavailability episode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BodyRetryEpisode {
    /// Exact selected branch whose body is unavailable.
    pub branch: BranchId,
    /// Selected-header generation that owns this episode.
    pub generation: HeaderGeneration,
    /// Exact header whose body is unavailable.
    pub header: Frontier,
    /// Authoritative local start time.
    pub started_at: DateTime<Utc>,
    /// Failed delivery attempts in this episode.
    pub attempts: u32,
    /// Suppliers already tried in this episode.
    pub tried_suppliers: BTreeSet<SourceId>,
    /// Earliest time a repeated supplier or alarm probe is eligible.
    pub next_probe_at: DateTime<Utc>,
    /// Whether persistent unavailability is visible.
    pub alarmed: bool,
    eligible_suppliers: BTreeSet<SourceId>,
}

impl BodyRetryEpisode {
    /// Start a fresh episode using an injected authoritative clock.
    pub fn new<C: Clock>(
        branch: BranchId,
        generation: HeaderGeneration,
        header: Frontier,
        eligible_suppliers: BTreeSet<SourceId>,
        clock: &C,
    ) -> Self {
        let now = clock.now();
        Self {
            branch,
            generation,
            header,
            started_at: now,
            attempts: 0,
            tried_suppliers: BTreeSet::new(),
            next_probe_at: now,
            alarmed: false,
            eligible_suppliers,
        }
    }

    /// Replace this episode when a newly eligible supplier can change availability.
    pub fn refresh_suppliers<C: Clock>(
        &mut self,
        eligible_suppliers: BTreeSet<SourceId>,
        clock: &C,
    ) -> bool {
        let has_new_supplier = eligible_suppliers
            .iter()
            .any(|supplier| !self.eligible_suppliers.contains(supplier));
        if has_new_supplier {
            *self = Self::new(
                self.branch,
                self.generation,
                self.header,
                eligible_suppliers,
                clock,
            );
        } else {
            self.eligible_suppliers = eligible_suppliers;
            self.tried_suppliers
                .retain(|supplier| self.eligible_suppliers.contains(supplier));
        }
        has_new_supplier
    }

    /// Start a new episode after an explicit operator retry command.
    pub fn restart<C: Clock>(&mut self, clock: &C) {
        *self = Self::new(
            self.branch,
            self.generation,
            self.header,
            self.eligible_suppliers.clone(),
            clock,
        );
    }

    /// Whether a repeated supplier attempt or persistent-alarm probe is due.
    pub fn is_due<C: Clock>(&self, clock: &C) -> bool {
        clock.now() >= self.next_probe_at
    }

    /// Record one failed supplier attempt and return its bounded scheduling consequence.
    pub fn record_failure<C: Clock, J: RetryJitter>(
        &mut self,
        supplier: SourceId,
        clock: &C,
        jitter: &J,
    ) -> RetryUpdate {
        let now = clock.now();
        if (self.alarmed || self.tried_suppliers.contains(&supplier)) && now < self.next_probe_at {
            return RetryUpdate::TooEarly;
        }

        self.attempts = self.attempts.saturating_add(1);
        self.tried_suppliers.insert(supplier);
        let all_suppliers_tried = !self.eligible_suppliers.is_empty()
            && self
                .eligible_suppliers
                .iter()
                .all(|supplier| self.tried_suppliers.contains(supplier));
        let alarm_due = self.attempts >= ALARM_ATTEMPTS
            || now.signed_duration_since(self.started_at) >= ALARM_AFTER;
        if self.alarmed {
            self.next_probe_at = now + ALARM_PROBE_INTERVAL;
            return RetryUpdate::ProbeAt(self.next_probe_at);
        }
        if all_suppliers_tried && alarm_due {
            self.alarmed = true;
            self.next_probe_at = now + ALARM_PROBE_INTERVAL;
            return RetryUpdate::Alarmed {
                probe_at: self.next_probe_at,
            };
        }

        self.next_probe_at = now + retry_delay(self.branch, self.header, self.attempts, jitter);
        RetryUpdate::RetryAt(self.next_probe_at)
    }

    /// Return the bounded durable alarm summary for state admission.
    pub fn summary(&self) -> BodyUnavailableSummary {
        BodyUnavailableSummary {
            attempts: self.attempts,
            suppliers: u32::try_from(self.eligible_suppliers.len()).unwrap_or(u32::MAX),
            alarmed: self.alarmed,
        }
    }
}

fn retry_delay<J: RetryJitter>(
    branch: BranchId,
    header: Frontier,
    attempt: u32,
    jitter: &J,
) -> Duration {
    let base_seconds = if attempt <= 6 {
        1_i64 << attempt.saturating_sub(1)
    } else {
        MAX_BACKOFF_SECONDS
    };
    let offset = jitter
        .offset_per_thousand(branch, header, attempt)
        .clamp(-MAX_JITTER_PER_THOUSAND, MAX_JITTER_PER_THOUSAND);
    let milliseconds = base_seconds
        .saturating_mul(1_000)
        .saturating_mul(i64::from(1_000_i16.saturating_add(offset)))
        / 1_000;
    Duration::milliseconds(milliseconds)
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
struct BodyRetryKey {
    generation: HeaderGeneration,
    branch: BranchId,
    header_hash: zakura_chain::block::Hash,
}

/// Generation- and branch-owned body retry work.
#[derive(Clone, Debug, Default)]
pub struct BodyRetryQueue(HashMap<BodyRetryKey, BodyRetryEpisode>);

impl BodyRetryQueue {
    /// Insert or replace one exact generation/branch/header episode.
    pub fn insert(&mut self, episode: BodyRetryEpisode) -> Option<BodyRetryEpisode> {
        self.0.insert(
            BodyRetryKey {
                generation: episode.generation,
                branch: episode.branch,
                header_hash: episode.header.hash,
            },
            episode,
        )
    }

    /// Return one exact current episode for scheduling or completion handling.
    pub fn get_mut(
        &mut self,
        generation: HeaderGeneration,
        branch: BranchId,
        header_hash: zakura_chain::block::Hash,
    ) -> Option<&mut BodyRetryEpisode> {
        self.0.get_mut(&BodyRetryKey {
            generation,
            branch,
            header_hash,
        })
    }

    /// Remove one exact completed or canceled episode.
    pub fn remove(
        &mut self,
        generation: HeaderGeneration,
        branch: BranchId,
        header_hash: zakura_chain::block::Hash,
    ) -> Option<BodyRetryEpisode> {
        self.0.remove(&BodyRetryKey {
            generation,
            branch,
            header_hash,
        })
    }

    /// Number of exact retry episodes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether no body retry episode remains scheduled.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Retire episodes outside the exact current generation and finalized anchor.
    pub fn retain_current(&mut self, generation: HeaderGeneration, finalized: Frontier) {
        self.0.retain(|key, _| {
            key.generation == generation && key.branch.anchor_hash == finalized.hash
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use zakura_chain::block;

    use super::*;

    struct ManualClock(Mutex<DateTime<Utc>>);

    impl ManualClock {
        fn new(now: DateTime<Utc>) -> Self {
            Self(Mutex::new(now))
        }

        fn advance(&self, duration: Duration) {
            let mut now = self.0.lock().expect("the test clock mutex is not poisoned");
            *now += duration;
        }
    }

    impl Clock for ManualClock {
        fn now(&self) -> DateTime<Utc> {
            *self.0.lock().expect("the test clock mutex is not poisoned")
        }
    }

    struct FixedJitter(i16);

    impl RetryJitter for FixedJitter {
        fn offset_per_thousand(&self, _branch: BranchId, _header: Frontier, _attempt: u32) -> i16 {
            self.0
        }
    }

    fn hash(byte: u8) -> block::Hash {
        block::Hash([byte; 32])
    }

    fn source(byte: u8) -> SourceId {
        SourceId::from_digest([byte; 32])
    }

    fn clock() -> ManualClock {
        ManualClock::new(
            DateTime::from_timestamp(1_700_000_000, 0).expect("the timestamp is valid"),
        )
    }

    fn episode(clock: &ManualClock, suppliers: &[SourceId]) -> BodyRetryEpisode {
        BodyRetryEpisode::new(
            BranchId::new(hash(1), hash(2)),
            HeaderGeneration::new(3),
            Frontier::new(block::Height(20), hash(2)),
            suppliers.iter().copied().collect(),
            clock,
        )
    }

    #[test]
    fn exact_backoff_caps_then_alarms_and_probes_every_ten_minutes() {
        let clock = clock();
        let supplier = source(3);
        let mut episode = episode(&clock, &[supplier]);
        let jitter = FixedJitter(0);

        for expected_seconds in [1, 2, 4, 8, 16, 32, 60, 60, 60] {
            let now = clock.now();
            assert_eq!(
                episode.record_failure(supplier, &clock, &jitter),
                RetryUpdate::RetryAt(now + Duration::seconds(expected_seconds))
            );
            clock.advance(Duration::seconds(expected_seconds));
        }
        let now = clock.now();
        assert_eq!(
            episode.record_failure(supplier, &clock, &jitter),
            RetryUpdate::Alarmed {
                probe_at: now + ALARM_PROBE_INTERVAL
            }
        );
        assert_eq!(
            episode.record_failure(supplier, &clock, &jitter),
            RetryUpdate::TooEarly
        );
        clock.advance(ALARM_PROBE_INTERVAL);
        let now = clock.now();
        assert_eq!(
            episode.record_failure(supplier, &clock, &jitter),
            RetryUpdate::ProbeAt(now + ALARM_PROBE_INTERVAL)
        );
        assert_eq!(
            episode.summary(),
            BodyUnavailableSummary {
                attempts: 11,
                suppliers: 1,
                alarmed: true
            }
        );
    }

    #[test]
    fn jitter_is_clamped_and_a_new_supplier_starts_a_fresh_episode() {
        let clock = clock();
        let first = source(3);
        let second = source(4);
        let mut episode = episode(&clock, &[first]);
        let now = clock.now();
        assert_eq!(
            episode.record_failure(first, &clock, &FixedJitter(-500)),
            RetryUpdate::RetryAt(now + Duration::milliseconds(900))
        );
        clock.advance(Duration::milliseconds(900));
        let now = clock.now();
        assert_eq!(
            episode.record_failure(first, &clock, &FixedJitter(500)),
            RetryUpdate::RetryAt(now + Duration::milliseconds(2_200))
        );
        assert!(episode.refresh_suppliers([first, second].into_iter().collect(), &clock));
        assert_eq!(episode.attempts, 0);
        assert!(episode.tried_suppliers.is_empty());
        assert!(!episode.alarmed);
        episode.record_failure(second, &clock, &FixedJitter(0));
        assert_ne!(episode.attempts, 0);
        episode.restart(&clock);
        assert_eq!(episode.attempts, 0);
        assert!(episode.is_due(&clock));
    }

    #[test]
    fn seeded_jitter_is_reproducible_and_within_the_normative_bound() {
        let episode = episode(&clock(), &[source(3)]);
        let jitter = SeededRetryJitter::new([7; 32]);
        let first = jitter.offset_per_thousand(episode.branch, episode.header, 1);
        assert_eq!(
            first,
            jitter.offset_per_thousand(episode.branch, episode.header, 1)
        );
        assert!((-100..=100).contains(&first));
        for attempt in 2..=64 {
            assert!((-100..=100).contains(&jitter.offset_per_thousand(
                episode.branch,
                episode.header,
                attempt
            )));
        }
    }

    #[test]
    fn elapsed_alarm_waits_until_every_known_supplier_was_tried() {
        let clock = clock();
        let first = source(3);
        let second = source(4);
        let mut episode = episode(&clock, &[first, second]);
        let jitter = FixedJitter(0);
        assert!(matches!(
            episode.record_failure(first, &clock, &jitter),
            RetryUpdate::RetryAt(_)
        ));
        clock.advance(ALARM_AFTER);
        assert!(matches!(
            episode.record_failure(first, &clock, &jitter),
            RetryUpdate::RetryAt(_)
        ));
        assert!(matches!(
            episode.record_failure(second, &clock, &jitter),
            RetryUpdate::Alarmed { .. }
        ));
    }

    #[test]
    fn generation_or_anchor_change_retires_retry_work_before_reuse() {
        let clock = clock();
        let episode = episode(&clock, &[source(3)]);
        let mut queue = BodyRetryQueue::default();
        assert!(queue.insert(episode.clone()).is_none());
        assert!(queue
            .get_mut(episode.generation, episode.branch, episode.header.hash)
            .is_some());
        queue.retain_current(
            HeaderGeneration::new(4),
            Frontier::new(block::Height(10), episode.branch.anchor_hash),
        );
        assert_eq!(queue.len(), 0);
        assert!(queue.is_empty());

        queue.insert(episode.clone());
        assert_eq!(
            queue.remove(episode.generation, episode.branch, episode.header.hash),
            Some(episode.clone())
        );
        queue.insert(episode.clone());
        queue.retain_current(
            episode.generation,
            Frontier::new(block::Height(11), hash(9)),
        );
        assert_eq!(queue.len(), 0);
    }
}
