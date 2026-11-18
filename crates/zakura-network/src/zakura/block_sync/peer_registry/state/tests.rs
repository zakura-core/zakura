//! Machine tests for the peer-registry owners.
//!
//! Each machine target drives the pure owners with decoded inputs. The retry
//! target checks the owners' invariants after every input and compares
//! lookups only with facts that follow from what it wrote: a fresh deferral
//! is avoided, nothing is avoided past the latest deadline written, a cleared
//! body is free, and a compatible refresh keeps every avoidance of its epoch.
//! Named scenarios pin the rekeying rules. The peer-table target keeps a small
//! bookkeeping model: each peer's generation, close flag and published claims.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
    time::Duration,
};

use zakura_header_chain::{BodyWorkEpoch, HeaderGeneration, HeaderWorkAuthority};

use super::*;
use crate::zakura::{
    block_sync::{test_work_scope, ZakuraBlockSyncConfig},
    machine_test::MAX_HISTORY_BYTES,
};

const SOURCES: u8 = 3;
const GENERATIONS: u64 = 3;
const EPOCHS: u64 = 3;
const HASHES: u8 = 3;

fn source(raw: u8) -> SourceId {
    SourceId::from_digest([raw + 1; 32])
}

fn hash(raw: u8) -> block::Hash {
    block::Hash([raw + 1; 32])
}

/// A work authority from its retry-key coordinates. The branch stays fixed.
fn authority((generation, epoch): (u64, u64)) -> BodyWorkAuthority {
    let base = test_work_scope();
    BodyWorkAuthority {
        header: HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(generation),
            ..base.header
        },
        body_work_epoch: BodyWorkEpoch::new(epoch),
        ..base
    }
}

// ---- body-retry avoidance ----

/// Retry-key coordinates: (header generation, body-work epoch).
type Scope = (u64, u64);

#[derive(Clone, Debug)]
enum RetryOp {
    DeferUnavailable {
        sources: Vec<u8>,
        scope: Scope,
        hash: u8,
        ttl: u64,
    },
    DeferIncompatible {
        source: u8,
        bodies: Vec<(Scope, u8)>,
        ttl: u64,
    },
    SetAlarm(Option<(Scope, u8, u64)>),
    Clear(Scope, u8),
    Retain(Option<Scope>),
    Refresh(Scope),
    Advance(u64),
    NextDeadline(u8),
}

fn scopes() -> impl Iterator<Item = Scope> {
    (0..GENERATIONS).flat_map(|generation| (0..EPOCHS).map(move |epoch| (generation, epoch)))
}

fn bodies() -> impl Iterator<Item = (Scope, u8)> {
    scopes().flat_map(|scope| (0..HASHES).map(move |hash| (scope, hash)))
}

/// Deadlines the test wrote, in seconds after the history's base instant.
#[derive(Debug, Default)]
struct Written {
    now: u64,
    /// The authority the last retain or refresh installed.
    current: Option<Scope>,
    /// Every supplier deadline written for each (source, hash).
    deferrals: BTreeMap<(u8, u8), BTreeSet<u64>>,
    /// Every alarm deadline written for each hash.
    alarms: BTreeMap<u8, BTreeSet<u64>>,
}

impl Written {
    /// The latest deadline that could still avoid `source` for `hash`.
    fn latest(&self, source: u8, hash: u8) -> u64 {
        let deferral = self.deferrals.get(&(source, hash)).and_then(BTreeSet::last);
        let alarm = self.alarms.get(&hash).and_then(BTreeSet::last);
        deferral.max(alarm).copied().unwrap_or(0)
    }

    /// Whether the test wrote `deadline` for `source` or for the alarm.
    fn wrote(&self, source: u8, deadline: u64) -> bool {
        self.deferrals
            .iter()
            .filter(|((candidate, _), _)| *candidate == source)
            .map(|(_, deadlines)| deadlines)
            .chain(self.alarms.values())
            .any(|deadlines| deadlines.contains(&deadline))
    }
}

/// A fact that follows from one input, checked after the step's hook.
#[derive(Debug)]
enum Fresh {
    /// The source avoids the body it was just deferred for.
    Avoided(u8, Scope, u8),
    /// The alarm just set avoids its body.
    Alarmed(Scope, u8),
    /// The alarm was just cleared.
    NoAlarm,
    /// No supplier avoids the body just cleared under the current authority.
    Cleared(Scope, u8),
    /// Work older than the authority just retained avoids nothing.
    Retained(Option<Scope>),
    /// A compatible refresh kept these avoidances of its epoch.
    Kept(BTreeSet<(u64, u8, Scope, u8)>),
}

/// Owners under test, with the base instant that test seconds count from.
struct RetryOwners {
    base: Instant,
    alarms: BodyRetryAlarms,
    suppliers: SupplierRetryState,
}

impl RetryOwners {
    fn at(&self, seconds: u64) -> Instant {
        self.base + Duration::from_secs(seconds)
    }

    fn avoided(&self, source: u8, scope: Scope, raw_hash: u8, at: u64) -> bool {
        is_body_retry_avoided(
            &self.alarms,
            &self.suppliers,
            self::source(source),
            authority(scope),
            hash(raw_hash),
            self.at(at),
        )
    }

    /// The same lookup with only the supplier deferrals.
    fn supplier_avoided(&self, source: u8, scope: Scope, raw_hash: u8, at: u64) -> bool {
        is_body_retry_avoided(
            &BodyRetryAlarms::default(),
            &self.suppliers,
            self::source(source),
            authority(scope),
            hash(raw_hash),
            self.at(at),
        )
    }

    /// The same lookup with only the alarm.
    fn alarm_avoided(&self, scope: Scope, raw_hash: u8, at: u64) -> bool {
        is_body_retry_avoided(
            &self.alarms,
            &SupplierRetryState::default(),
            source(0),
            authority(scope),
            hash(raw_hash),
            self.at(at),
        )
    }

    /// Every (source, scope, hash) lookup at `now` and at a later instant.
    fn avoidances(&self, now: u64) -> BTreeSet<(u64, u8, Scope, u8)> {
        let mut avoided = BTreeSet::new();
        for at in [now, now + 7] {
            for raw_source in 0..SOURCES {
                for scope in scopes() {
                    for raw_hash in 0..HASHES {
                        if self.avoided(raw_source, scope, raw_hash, at) {
                            avoided.insert((at, raw_source, scope, raw_hash));
                        }
                    }
                }
            }
        }
        avoided
    }

    /// Apply `op`. Returns the facts that follow directly from it.
    fn apply(&mut self, written: &mut Written, op: RetryOp) -> Vec<Fresh> {
        let now = written.now;
        let mut fresh = Vec::new();
        match op {
            RetryOp::DeferUnavailable {
                sources,
                scope,
                hash: raw_hash,
                ttl,
            } => {
                let skipped = self.suppliers.defer_unavailable(
                    sources.iter().copied().map(source).collect(),
                    authority(scope),
                    hash(raw_hash),
                    self.at(now + ttl),
                );
                assert_eq!(skipped, 0, "{RETRY_PROPERTY}");
                for raw_source in &sources {
                    written
                        .deferrals
                        .entry((*raw_source, raw_hash))
                        .or_default()
                        .insert(now + ttl);
                }
                fresh.extend(
                    sources
                        .into_iter()
                        .map(|raw_source| Fresh::Avoided(raw_source, scope, raw_hash)),
                );
            }
            RetryOp::DeferIncompatible {
                source: raw_source,
                bodies,
                ttl,
            } => {
                let skipped = self.suppliers.defer_incompatible(
                    source(raw_source),
                    bodies
                        .iter()
                        .map(|(scope, raw_hash)| (authority(*scope), hash(*raw_hash)))
                        .collect(),
                    self.at(now + ttl),
                    self.at(now),
                );
                assert_eq!(skipped, 0, "{RETRY_PROPERTY}");
                for (scope, raw_hash) in bodies {
                    written
                        .deferrals
                        .entry((raw_source, raw_hash))
                        .or_default()
                        .insert(now + ttl);
                    // Work older than the current authority defers nothing.
                    if written.current.is_none_or(|current| scope.1 >= current.1) {
                        fresh.push(Fresh::Avoided(raw_source, scope, raw_hash));
                    }
                }
            }
            RetryOp::SetAlarm(alarm) => {
                self.alarms.set(alarm.map(|(scope, raw_hash, ttl)| {
                    (authority(scope), hash(raw_hash), self.at(now + ttl))
                }));
                match alarm {
                    Some((scope, raw_hash, ttl)) => {
                        written
                            .alarms
                            .entry(raw_hash)
                            .or_default()
                            .insert(now + ttl);
                        fresh.push(Fresh::Alarmed(scope, raw_hash));
                    }
                    None => fresh.push(Fresh::NoAlarm),
                }
            }
            RetryOp::Clear(scope, raw_hash) => {
                self.suppliers.clear(authority(scope), hash(raw_hash));
                self.alarms.clear(authority(scope), hash(raw_hash));
                // Under the authority the test installed, the body is free of
                // every supplier deferral.
                if written.current.is_none_or(|current| current == scope) {
                    fresh.push(Fresh::Cleared(scope, raw_hash));
                }
            }
            RetryOp::Retain(current) => {
                written.current = current;
                retain_body_retry_scope(
                    &mut self.alarms,
                    &mut self.suppliers,
                    current.map(authority),
                );
                fresh.push(Fresh::Retained(current));
            }
            RetryOp::Refresh(current) => {
                written.current = Some(current);
                let mut before = self.avoidances(now);
                before.retain(|(_, _, scope, _)| scope.1 == current.1);
                refresh_body_retry_scope(&mut self.alarms, &mut self.suppliers, authority(current));
                fresh.push(Fresh::Kept(before));
            }
            RetryOp::Advance(seconds) => written.now += seconds,
            RetryOp::NextDeadline(raw_source) => {
                let all = self.alarms.next_deadline(self.at(now));
                let supplier = self
                    .suppliers
                    .next_deadline(source(raw_source), self.at(now));
                match all.into_iter().chain(supplier).min() {
                    Some(deadline) => {
                        let seconds = deadline.duration_since(self.base).as_secs();
                        assert!(
                            seconds > now && written.wrote(raw_source, seconds),
                            "{RETRY_PROPERTY}: {raw_source}'s next deadline {seconds} was not written after {now}"
                        );
                    }
                    None => {
                        for (scope, raw_hash) in bodies() {
                            assert!(
                                !self.avoided(raw_source, scope, raw_hash, now),
                                "{RETRY_PROPERTY}: {raw_source} has no deadline but avoids {raw_hash} under {scope:?}"
                            );
                        }
                    }
                }
            }
        }
        fresh
    }

    /// Check each fact that followed from the step's input.
    fn check_fresh(&self, now: u64, fresh: Vec<Fresh>) {
        for fact in fresh {
            let holds = match &fact {
                Fresh::Avoided(raw_source, scope, raw_hash) => {
                    self.avoided(*raw_source, *scope, *raw_hash, now)
                }
                Fresh::Alarmed(scope, raw_hash) => self.alarm_avoided(*scope, *raw_hash, now),
                Fresh::NoAlarm => {
                    bodies().all(|(scope, raw_hash)| !self.alarm_avoided(scope, raw_hash, now))
                }
                Fresh::Cleared(scope, raw_hash) => (0..SOURCES)
                    .all(|raw_source| !self.supplier_avoided(raw_source, *scope, *raw_hash, now)),
                Fresh::Retained(current) => {
                    self.check_stale_work_is_retired(*current);
                    true
                }
                Fresh::Kept(before) => before.is_subset(&self.avoidances(now)),
            };
            assert!(holds, "{RETRY_PROPERTY}: {fact:?} does not hold");
        }
    }

    /// Right after a retain, work from an older epoch avoids nothing, and a
    /// retain to `None` clears every deferral and alarm.
    fn check_stale_work_is_retired(&self, current: Option<Scope>) {
        for scope in scopes() {
            if current.is_some_and(|current| scope.1 >= current.1) {
                continue;
            }
            for raw_source in 0..SOURCES {
                for raw_hash in 0..HASHES {
                    assert!(
                        !self.avoided(raw_source, scope, raw_hash, 0),
                        "{RETRY_PROPERTY}: stale scope {scope:?} still avoids after retaining {current:?}");
                }
            }
        }
    }

    /// Check the owners' invariants, and that nothing is avoided at or past
    /// the latest deadline the test wrote for it.
    fn check(&self, written: &Written) {
        self.alarms.assert_invariants();
        self.suppliers.assert_invariants();
        for (at, raw_source, scope, raw_hash) in self.avoidances(written.now) {
            assert!(
                at < written.latest(raw_source, raw_hash),
                "{RETRY_PROPERTY}: source {raw_source} scope {scope:?} hash {raw_hash} is avoided at {at}, past every deadline written"
            );
        }
    }
}

const RETRY_PROPERTY: &str = "block/retry-authority";

/// Applicable retry operations in a decoder-v1 history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct RetryAuthorityReplaySummary {
    /// Operations checked.
    pub transitions: usize,
    /// Deferral writes checked.
    pub deferrals: usize,
    /// Retain or refresh operations checked.
    pub scope_changes: usize,
    /// Nonzero logical-time advances checked.
    pub time_advances: usize,
    /// Next-deadline queries checked.
    pub deadline_queries: usize,
}

/// Replay at most [`MAX_HISTORY_BYTES`] decoder-v1 bytes through the supplier
/// and alarm owners. Each quadruple is one input. Time is relative to one
/// origin per history.
pub fn replay_retry_authority_bytes(data: &[u8]) -> RetryAuthorityReplaySummary {
    run_retry(decode_retry(data), |_| {})
}

fn decode_retry(data: &[u8]) -> impl Iterator<Item = RetryOp> + '_ {
    data[..data.len().min(MAX_HISTORY_BYTES)]
        .chunks(4)
        .map(|chunk| {
            let a = chunk[0];
            let b = chunk.get(1).copied().unwrap_or(0);
            let c = chunk.get(2).copied().unwrap_or(0);
            let d = chunk.get(3).copied().unwrap_or(0);
            let scope = (u64::from(b) % GENERATIONS, u64::from(b / 3) % EPOCHS);
            let hash = c % HASHES;
            let ttl = u64::from(d % 20) + 1;
            match a % 8 {
                0 => RetryOp::DeferUnavailable {
                    sources: (0..SOURCES)
                        .filter(|source| (c / 3) & (1 << source) != 0)
                        .collect(),
                    scope,
                    hash,
                    ttl,
                },
                1 => RetryOp::DeferIncompatible {
                    source: c / 3 % SOURCES,
                    bodies: vec![(scope, hash)],
                    ttl,
                },
                2 => RetryOp::SetAlarm((a & 128 == 0).then_some((scope, hash, ttl))),
                3 => RetryOp::Clear(scope, hash),
                4 => RetryOp::Retain((a & 128 == 0).then_some(scope)),
                5 => RetryOp::Refresh(scope),
                6 => RetryOp::Advance(u64::from(d)),
                _ => RetryOp::NextDeadline(c % SOURCES),
            }
        })
}

fn run_retry(
    ops: impl IntoIterator<Item = RetryOp>,
    mut after_step: impl FnMut(&mut RetryOwners),
) -> RetryAuthorityReplaySummary {
    let mut owners = RetryOwners {
        base: Instant::now(),
        alarms: BodyRetryAlarms::default(),
        suppliers: SupplierRetryState::default(),
    };
    let mut written = Written::default();
    let mut summary = RetryAuthorityReplaySummary::default();
    for op in ops {
        summary.transitions += 1;
        summary.deferrals += usize::from(
            matches!(&op, RetryOp::DeferUnavailable { sources, .. } if !sources.is_empty())
                || matches!(&op, RetryOp::DeferIncompatible { bodies, .. } if !bodies.is_empty()),
        );
        summary.scope_changes +=
            usize::from(matches!(op, RetryOp::Retain(_) | RetryOp::Refresh(_)));
        summary.time_advances +=
            usize::from(matches!(op, RetryOp::Advance(seconds) if seconds > 0));
        summary.deadline_queries += usize::from(matches!(op, RetryOp::NextDeadline(_)));
        let fresh = owners.apply(&mut written, op);
        after_step(&mut owners);
        owners.check_fresh(written.now, fresh);
        owners.check(&written);
    }
    summary
}

/// Defers one supplier's unavailability and another's incompatibility,
/// refreshes, retains a newer epoch, sets an alarm, advances time and reads a
/// deadline.
#[cfg(test)]
const RETRY_WITNESS: &[u8] = &[
    0, 0, 3, 2, 1, 0, 3, 3, 5, 1, 0, 0, 4, 4, 0, 0, 2, 4, 0, 2, 6, 0, 0, 5, 7, 0, 0, 0,
];

#[cfg(test)]
mod retry_target {
    use super::*;
    use crate::zakura::machine_test::machine_target_tests;

    machine_target_tests! {
        replay: replay_retry_authority_bytes,
        target: "block_retry_authority",
        decoder: 1,
        witness: RETRY_WITNESS => RetryAuthorityReplaySummary {
            transitions: 7,
            deferrals: 2,
            scope_changes: 2,
            time_advances: 1,
            deadline_queries: 1,
        },
    }
}

#[test]
#[should_panic(expected = "block/retry-authority")]
fn retry_witness_rejects_lost_supplier_deferral() {
    run_retry(decode_retry(RETRY_WITNESS), |owners| {
        owners.suppliers = SupplierRetryState::default();
    });
}

#[test]
#[should_panic(expected = "block/retry-authority")]
fn retry_witness_rejects_lost_alarm() {
    run_retry(decode_retry(RETRY_WITNESS), |owners| {
        owners.alarms = BodyRetryAlarms::default();
    });
}

/// Fresh owners and the instant `seconds` after their base.
#[cfg(test)]
fn retry_owners() -> (BodyRetryAlarms, SupplierRetryState, impl Fn(u64) -> Instant) {
    let base = Instant::now();
    (
        BodyRetryAlarms::default(),
        SupplierRetryState::default(),
        move |seconds| base + Duration::from_secs(seconds),
    )
}

#[test]
fn incompatible_report_from_stale_work_cannot_suppress_current_work() {
    let (mut alarms, mut suppliers, at) = retry_owners();
    retain_body_retry_scope(&mut alarms, &mut suppliers, Some(authority((0, 1))));
    assert_eq!(
        suppliers.defer_incompatible(source(0), vec![(authority((0, 0)), hash(0))], at(60), at(0)),
        0,
    );
    for scope in [(0, 1), (0, 0)] {
        assert!(!is_body_retry_avoided(
            &alarms,
            &suppliers,
            source(0),
            authority(scope),
            hash(0),
            at(0)
        ));
    }
    assert_eq!(suppliers.len(), 0);
}

/// Rekeying merges two deadlines of one cooldown; the later one survives.
#[test]
fn a_rekey_merge_keeps_the_latest_deadline() {
    for (first, second) in [(10, 30), (30, 10)] {
        let (mut alarms, mut suppliers, at) = retry_owners();
        let _ = suppliers.defer_unavailable(
            BTreeSet::from([source(0)]),
            authority((0, 0)),
            hash(0),
            at(first),
        );
        let _ = suppliers.defer_unavailable(
            BTreeSet::from([source(0)]),
            authority((1, 0)),
            hash(0),
            at(second),
        );
        refresh_body_retry_scope(&mut alarms, &mut suppliers, authority((2, 0)));
        assert_eq!(suppliers.len(), 1);
        assert_eq!(suppliers.next_deadline(source(0), at(0)), Some(at(30)));
    }
}

/// Deferring a body again installs the exact supplier set: a departed
/// supplier's unavailability ends, and a repeated one takes the new deadline.
#[test]
fn an_unavailable_deferral_installs_the_exact_supplier_set() {
    let (alarms, mut suppliers, at) = retry_owners();
    let scope = authority((0, 0));
    let _ = suppliers.defer_unavailable(
        BTreeSet::from([source(0), source(1)]),
        scope,
        hash(0),
        at(30),
    );
    let _ = suppliers.defer_unavailable(BTreeSet::from([source(1)]), scope, hash(0), at(10));
    let avoided = |source, seconds| {
        is_body_retry_avoided(&alarms, &suppliers, source, scope, hash(0), at(seconds))
    };
    assert!(!avoided(source(0), 0));
    assert!(avoided(source(1), 9));
    assert!(!avoided(source(1), 10));
}

/// Another incompatibility report extends a cooldown and never shortens it.
#[test]
fn an_incompatibility_report_only_extends_its_cooldown() {
    let (alarms, mut suppliers, at) = retry_owners();
    let scope = authority((0, 0));
    for until in [30, 10] {
        let _ = suppliers.defer_incompatible(source(0), vec![(scope, hash(0))], at(until), at(0));
    }
    assert!(is_body_retry_avoided(
        &alarms,
        &suppliers,
        source(0),
        scope,
        hash(0),
        at(29)
    ));
    assert!(!is_body_retry_avoided(
        &alarms,
        &suppliers,
        source(0),
        scope,
        hash(0),
        at(30)
    ));
}

/// A retain keeps an unavailability only under its exact authority; a
/// compatible refresh carries it to the new one.
#[test]
fn a_retain_drops_and_a_refresh_carries_an_unavailability() {
    for refresh in [false, true] {
        let (mut alarms, mut suppliers, at) = retry_owners();
        let _ = suppliers.defer_unavailable(
            BTreeSet::from([source(0)]),
            authority((0, 0)),
            hash(0),
            at(30),
        );
        let next = authority((1, 0));
        if refresh {
            refresh_body_retry_scope(&mut alarms, &mut suppliers, next);
        } else {
            retain_body_retry_scope(&mut alarms, &mut suppliers, Some(next));
        }
        assert_eq!(
            is_body_retry_avoided(&alarms, &suppliers, source(0), next, hash(0), at(0)),
            refresh,
            "refresh: {refresh}"
        );
    }
}

/// A refresh into a new epoch carries an unavailability, which names
/// suppliers, but not an incompatibility, which names the old work.
#[test]
fn a_refresh_across_epochs_carries_only_an_unavailability() {
    let (mut alarms, mut suppliers, at) = retry_owners();
    let old = authority((0, 0));
    let _ = suppliers.defer_unavailable(BTreeSet::from([source(0)]), old, hash(0), at(30));
    let _ = suppliers.defer_incompatible(source(1), vec![(old, hash(0))], at(30), at(0));
    let new = authority((0, 1));
    refresh_body_retry_scope(&mut alarms, &mut suppliers, new);
    let avoided =
        |raw| is_body_retry_avoided(&alarms, &suppliers, source(raw), new, hash(0), at(0));
    assert!(avoided(0));
    assert!(!avoided(1));
}

/// A lookup under an older snapshot of the current epoch finds a deferral
/// keyed under the current authority.
#[test]
fn a_lookup_checks_the_current_authority_of_its_epoch() {
    let (mut alarms, mut suppliers, at) = retry_owners();
    refresh_body_retry_scope(&mut alarms, &mut suppliers, authority((1, 0)));
    let _ =
        suppliers.defer_incompatible(source(0), vec![(authority((0, 0)), hash(0))], at(30), at(0));
    for scope in [(0, 0), (1, 0), (2, 0)] {
        assert!(
            is_body_retry_avoided(
                &alarms,
                &suppliers,
                source(0),
                authority(scope),
                hash(0),
                at(0)
            ),
            "{scope:?}"
        );
    }
    assert!(!is_body_retry_avoided(
        &alarms,
        &suppliers,
        source(0),
        authority((1, 1)),
        hash(0),
        at(0)
    ));
}

// ---- peer summaries ----

#[derive(Debug, Default)]
struct PeerModel {
    generation: u64,
    closed: bool,
    /// Height → (request ID, hash).
    outstanding: BTreeMap<u32, (u64, u8)>,
}

#[derive(Debug, Default)]
struct TableModel {
    next_generation: u64,
    peers: BTreeMap<u8, PeerModel>,
}

#[derive(Clone, Debug)]
enum TableOp {
    Admit(u8),
    /// Generations count back from the newest: 0 is current, 1 the one before.
    Insert {
        peer: u8,
        age: u64,
        request: u64,
        heights: Vec<u32>,
        hash: u8,
    },
    RemoveForOwner {
        peer: u8,
        age: u64,
        request: u64,
        heights: Vec<u32>,
    },
    Take {
        peer: u8,
        age: u64,
    },
    ClearHeight {
        peer: u8,
        height: u32,
        request: u64,
    },
    Close {
        peer: u8,
        age: u64,
    },
    RemoveClosed(u8),
}

const PEERS: u8 = 3;
const HEIGHTS: u32 = 6;

fn peer_id(raw: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![raw + 1; 32]).expect("32-byte test peer id is valid")
}

/// The request owner of `peer`'s request `request`.
fn owner(peer: u8, request: u64) -> BodyWorkOwner {
    test_work_scope().bind(
        u64::from(peer),
        NonZeroU64::new(request).expect("generated request ids are nonzero"),
    )
}

fn meta(owner: BodyWorkOwner, raw_hash: u8, at: Instant) -> OutstandingMeta {
    OutstandingMeta {
        owner,
        hash: hash(raw_hash),
        estimated_bytes: 1,
        queued_at: at,
        deadline: at,
    }
}

impl TableModel {
    /// The generation `age` admissions before `peer`'s current one.
    fn generation(&self, peer: u8, age: u64) -> u64 {
        self.peers
            .get(&peer)
            .map_or(0, |entry| entry.generation.saturating_sub(age))
    }

    fn owned(&mut self, peer: u8, generation: u64) -> Option<&mut PeerModel> {
        self.peers
            .get_mut(&peer)
            .filter(|entry| entry.generation == generation)
    }
}

fn apply_table(table: &mut PeerTable, model: &mut TableModel, op: TableOp, at: Instant) {
    let config = ZakuraBlockSyncConfig::default();
    match op {
        TableOp::Admit(peer) => {
            model.next_generation += 1;
            let generation = model.next_generation;
            let replaced = model.peers.insert(
                peer,
                PeerModel {
                    generation,
                    ..PeerModel::default()
                },
            );
            let removed = table.admit(
                &peer_id(peer),
                Entry::new(ServicePeerDirection::Outbound, &config, generation),
            );
            assert_eq!(
                removed.map(|entry| entry.generation),
                replaced.map(|entry| entry.generation),
                "{TABLE_PROPERTY}"
            );
        }
        TableOp::Insert {
            peer,
            age,
            request,
            heights,
            hash: raw_hash,
        } => {
            let generation = model.generation(peer, age);
            if let Some(entry) = model.owned(peer, generation) {
                for height in &heights {
                    entry.outstanding.insert(*height, (request, raw_hash));
                }
            }
            table.insert_outstanding(
                &peer_id(peer),
                generation,
                heights.iter().map(|height| {
                    (
                        block::Height(*height),
                        meta(owner(peer, request), raw_hash, at),
                    )
                }),
            );
        }
        TableOp::RemoveForOwner {
            peer,
            age,
            request,
            heights,
        } => {
            let generation = model.generation(peer, age);
            if let Some(entry) = model.owned(peer, generation) {
                for height in &heights {
                    if entry
                        .outstanding
                        .get(height)
                        .is_some_and(|(id, _)| *id == request)
                    {
                        entry.outstanding.remove(height);
                    }
                }
            }
            table.remove_outstanding_for_owner(
                &peer_id(peer),
                generation,
                owner(peer, request),
                heights.iter().copied().map(block::Height),
            );
        }
        TableOp::Take { peer, age } => {
            let generation = model.generation(peer, age);
            let expected = model
                .owned(peer, generation)
                .map(|entry| std::mem::take(&mut entry.outstanding).len());
            let detached = table.take_outstanding(&peer_id(peer), generation);
            assert_eq!(
                detached.map(|claims| claims.len()),
                expected,
                "{TABLE_PROPERTY}"
            );
        }
        TableOp::ClearHeight {
            peer,
            height,
            request,
        } => {
            let expected = model.peers.get_mut(&peer).is_some_and(|entry| {
                let owned = entry
                    .outstanding
                    .get(&height)
                    .is_some_and(|(id, _)| *id == request);
                if owned {
                    entry.outstanding.remove(&height);
                }
                owned
            });
            assert_eq!(
                table.clear_outstanding_height_for_owner(
                    &peer_id(peer),
                    block::Height(height),
                    owner(peer, request),
                ),
                expected,
                "{TABLE_PROPERTY}"
            );
        }
        TableOp::Close { peer, age } => {
            let generation = model.generation(peer, age);
            if let Some(entry) = model.owned(peer, generation) {
                entry.closed = true;
            }
            table.close_session(&peer_id(peer), generation);
        }
        TableOp::RemoveClosed(peer) => {
            let expected = model
                .peers
                .get(&peer)
                .is_some_and(|entry| entry.closed)
                .then(|| model.peers.remove(&peer))
                .flatten();
            let removed = table.remove_closed_session(&peer_id(peer));
            assert_eq!(
                removed.map(|entry| entry.generation),
                expected.map(|entry| entry.generation),
                "{TABLE_PROPERTY}"
            );
        }
    }
}

fn check_table(table: &PeerTable, model: &TableModel) {
    let total: usize = model
        .peers
        .values()
        .map(|entry| entry.outstanding.len())
        .sum();
    assert_eq!(table.total_unreceived(), total, "{TABLE_PROPERTY}");
    for height in 0..HEIGHTS {
        let claims = table.outstanding_claims_at(block::Height(height));
        let expected: Vec<_> = model
            .peers
            .iter()
            .filter_map(|(peer, entry)| {
                entry.outstanding.get(&height).map(|(request, raw_hash)| {
                    (peer_id(*peer), owner(*peer, *request), hash(*raw_hash))
                })
            })
            .collect();
        assert_eq!(
            claims.len(),
            expected.len(),
            "{TABLE_PROPERTY}: claims at height {height}"
        );
        for (peer, owner, hash) in expected {
            assert!(
                claims.iter().any(|claim| {
                    claim.peer == peer && claim.meta.owner == owner && claim.meta.hash == hash
                }),
                "{TABLE_PROPERTY}"
            );
            assert!(
                table.has_outstanding_request(block::Height(height), hash),
                "{TABLE_PROPERTY}"
            );
        }
        for peer in 0..PEERS {
            assert_eq!(
                table.peer_has_outstanding_height(&peer_id(peer), block::Height(height)),
                model
                    .peers
                    .get(&peer)
                    .is_some_and(|entry| entry.outstanding.contains_key(&height)),
                "{TABLE_PROPERTY}"
            );
        }
    }
    for peer in 0..PEERS {
        assert_eq!(
            table
                .get(&peer_id(peer))
                .map(|entry| (entry.generation, entry.session_closed)),
            model
                .peers
                .get(&peer)
                .map(|entry| (entry.generation, entry.closed)),
            "{TABLE_PROPERTY}"
        );
    }
}

const TABLE_PROPERTY: &str = "block/peer-registry";

/// Applicable peer-table operations in a decoder-v1 history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct PeerRegistryReplaySummary {
    /// Operations compared with the bookkeeping model.
    pub transitions: usize,
    /// Session admissions checked.
    pub admissions: usize,
    /// Admissions that replaced a session.
    pub replacements: usize,
    /// Operations naming an older admitted generation.
    pub stale_generations: usize,
    /// Owner-specific removal attempts against a present peer.
    pub owner_checks: usize,
}

/// Replay at most [`MAX_HISTORY_BYTES`] decoder-v1 bytes through the peer
/// table. Each quadruple is one input; generation and owner identities stay
/// exact.
pub fn replay_peer_registry_bytes(data: &[u8]) -> PeerRegistryReplaySummary {
    run_table(decode_table(data), |_, _| {})
}

fn decode_table(data: &[u8]) -> impl Iterator<Item = TableOp> + '_ {
    data[..data.len().min(MAX_HISTORY_BYTES)]
        .chunks(4)
        .map(|chunk| {
            let a = chunk[0];
            let b = chunk.get(1).copied().unwrap_or(0);
            let c = chunk.get(2).copied().unwrap_or(0);
            let d = chunk.get(3).copied().unwrap_or(0);
            let peer = b % PEERS;
            let age = u64::from(b / PEERS % 2);
            let request = u64::from(d % 3) + 1;
            let heights = || {
                (0..HEIGHTS)
                    .filter(|height| c & (1 << height) != 0)
                    .collect()
            };
            match a % 7 {
                0 => TableOp::Admit(peer),
                1 => TableOp::Insert {
                    peer,
                    age,
                    request,
                    heights: heights(),
                    hash: d / 3 % HASHES,
                },
                2 => TableOp::RemoveForOwner {
                    peer,
                    age,
                    request,
                    heights: heights(),
                },
                3 => TableOp::Take { peer, age },
                4 => TableOp::ClearHeight {
                    peer,
                    height: u32::from(c) % HEIGHTS,
                    request,
                },
                5 => TableOp::Close { peer, age },
                _ => TableOp::RemoveClosed(peer),
            }
        })
}

fn run_table(
    ops: impl IntoIterator<Item = TableOp>,
    mut after_step: impl FnMut(&mut PeerTable, &TableModel),
) -> PeerRegistryReplaySummary {
    let at = Instant::now();
    let mut table = PeerTable::default();
    let mut model = TableModel::default();
    let mut summary = PeerRegistryReplaySummary::default();
    for op in ops {
        summary.transitions += 1;
        match &op {
            TableOp::Admit(peer) => {
                summary.admissions += 1;
                summary.replacements += usize::from(model.peers.contains_key(peer));
            }
            TableOp::Insert { peer, age, .. }
            | TableOp::Take { peer, age }
            | TableOp::Close { peer, age } => {
                summary.stale_generations +=
                    usize::from(*age > 0 && model.peers.contains_key(peer));
            }
            TableOp::RemoveForOwner { peer, age, .. } => {
                summary.stale_generations +=
                    usize::from(*age > 0 && model.peers.contains_key(peer));
                summary.owner_checks += usize::from(model.peers.contains_key(peer));
            }
            TableOp::ClearHeight { peer, .. } => {
                summary.owner_checks += usize::from(model.peers.contains_key(peer))
            }
            TableOp::RemoveClosed(_) => {}
        }
        apply_table(&mut table, &mut model, op, at);
        after_step(&mut table, &model);
        check_table(&table, &model);
    }
    summary
}

/// Admits two peers, replaces one, inserts and removes claims under current
/// and stale generations, takes, closes and removes a closed session.
#[cfg(test)]
const TABLE_WITNESS: &[u8] = &[
    0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 3, 3, 0, 0, 5, 3, 0, 0, 4, 0, 0, 1, 4, 0, 0, 0,
    5, 0, 0, 0, 6, 0, 0, 0,
];

#[cfg(test)]
mod table_target {
    use super::*;
    use crate::zakura::machine_test::machine_target_tests;

    machine_target_tests! {
        replay: replay_peer_registry_bytes,
        target: "block_peer_registry",
        decoder: 1,
        witness: TABLE_WITNESS => PeerRegistryReplaySummary {
            transitions: 10,
            admissions: 2,
            replacements: 1,
            stale_generations: 2,
            owner_checks: 2,
        },
    }
}

#[test]
#[should_panic(expected = "block/peer-registry")]
fn registry_witness_rejects_lost_outstanding_claim() {
    run_table(decode_table(TABLE_WITNESS), |table, model| {
        for (peer, entry) in &model.peers {
            table.take_outstanding(&peer_id(*peer), entry.generation);
        }
    });
}

#[test]
#[should_panic(expected = "block/peer-registry")]
fn registry_witness_rejects_closing_current_session() {
    run_table(decode_table(TABLE_WITNESS), |table, model| {
        for (peer, entry) in &model.peers {
            table.close_session(&peer_id(*peer), entry.generation);
        }
    });
}
