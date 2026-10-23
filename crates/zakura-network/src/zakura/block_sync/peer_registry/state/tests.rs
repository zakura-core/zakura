//! Machine tests for the peer-registry owners.
//!
//! Each proptest drives a pure owner with generated inputs and compares every
//! observation with an independent model after each step. The models keep flat
//! record lists and never call the owners' helpers.

use std::{collections::BTreeMap, num::NonZeroU64, time::Duration};

#[cfg(test)]
use proptest::prelude::*;
use zakura_header_chain::{BodyWorkEpoch, HeaderGeneration, HeaderWorkAuthority};

use super::*;
use crate::zakura::block_sync::{test_work_scope, ZakuraBlockSyncConfig};

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

/// One supplier deferral in the model.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Deferral {
    source: u8,
    scope: Scope,
    hash: u8,
    incompatible: bool,
    /// Seconds after the test's base instant.
    until: u64,
}

impl Deferral {
    fn same_key(&self, other: &Self) -> bool {
        self.source == other.source
            && self.scope == other.scope
            && self.hash == other.hash
            && self.incompatible == other.incompatible
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Alarm {
    scope: Scope,
    hash: u8,
    until: u64,
}

#[derive(Debug, Default)]
struct RetryModel {
    now: u64,
    current: Option<Scope>,
    deferrals: Vec<Deferral>,
    alarm: Option<Alarm>,
}

impl RetryModel {
    fn find(
        &mut self,
        source: u8,
        scope: Scope,
        hash: u8,
        incompatible: bool,
    ) -> Option<&mut Deferral> {
        self.deferrals.iter_mut().find(|deferral| {
            deferral.source == source
                && deferral.scope == scope
                && deferral.hash == hash
                && deferral.incompatible == incompatible
        })
    }

    /// Merge deferrals that rekeying made identical, keeping the latest deadline.
    fn merge(&mut self) {
        let mut merged: Vec<Deferral> = Vec::new();
        for deferral in std::mem::take(&mut self.deferrals) {
            match merged.iter_mut().find(|kept| kept.same_key(&deferral)) {
                Some(kept) => kept.until = kept.until.max(deferral.until),
                None => merged.push(deferral),
            }
        }
        self.deferrals = merged;
    }

    fn defer_unavailable(&mut self, sources: &[u8], scope: Scope, hash: u8, until: u64) {
        self.deferrals.retain(|deferral| {
            deferral.incompatible
                || deferral.scope != scope
                || deferral.hash != hash
                || sources.contains(&deferral.source)
        });
        for source in sources {
            match self.find(*source, scope, hash, false) {
                Some(deferral) => deferral.until = until,
                None => self.deferrals.push(Deferral {
                    source: *source,
                    scope,
                    hash,
                    incompatible: false,
                    until,
                }),
            }
        }
    }

    fn defer_incompatible(&mut self, source: u8, bodies: &[(Scope, u8)], until: u64) {
        let now = self.now;
        self.deferrals.retain(|deferral| deferral.until > now);
        for (scope, hash) in bodies {
            let scope = match self.current {
                Some(current) if current.1 == scope.1 => current,
                Some(current) if current.1 > scope.1 => continue,
                _ => *scope,
            };
            match self.find(source, scope, *hash, true) {
                Some(deferral) => deferral.until = deferral.until.max(until),
                None => self.deferrals.push(Deferral {
                    source,
                    scope,
                    hash: *hash,
                    incompatible: true,
                    until,
                }),
            }
        }
    }

    fn clear(&mut self, scope: Scope, hash: u8) {
        self.deferrals
            .retain(|deferral| deferral.scope != scope || deferral.hash != hash);
        if self
            .alarm
            .is_some_and(|alarm| alarm.scope == scope && alarm.hash == hash)
        {
            self.alarm = None;
        }
    }

    fn retain(&mut self, current: Option<Scope>) {
        self.current = current;
        let Some(current) = current else {
            self.deferrals.clear();
            self.alarm = None;
            return;
        };
        self.deferrals.retain_mut(|deferral| {
            if !deferral.incompatible {
                return deferral.scope == current;
            }
            if deferral.scope.1 < current.1 {
                return false;
            }
            if deferral.scope.1 == current.1 {
                deferral.scope = current;
            }
            true
        });
        self.merge();
        self.alarm = self.alarm.and_then(|mut alarm| {
            if alarm.scope.1 < current.1 {
                return None;
            }
            if alarm.scope.1 == current.1 {
                alarm.scope = current;
            }
            Some(alarm)
        });
    }

    fn refresh(&mut self, current: Scope) {
        self.current = Some(current);
        if let Some(alarm) = &mut self.alarm {
            if alarm.scope.1 == current.1 {
                alarm.scope = current;
            }
        }
        for deferral in &mut self.deferrals {
            if !deferral.incompatible || deferral.scope.1 == current.1 {
                deferral.scope = current;
            }
        }
        self.merge();
    }

    fn is_avoided(&self, source: u8, scope: Scope, hash: u8, at: u64) -> bool {
        let current = self
            .current
            .filter(|current| current.1 == scope.1)
            .unwrap_or(scope);
        self.alarm
            .is_some_and(|alarm| alarm.until > at && alarm.hash == hash && alarm.scope.1 == scope.1)
            || self.deferrals.iter().any(|deferral| {
                deferral.source == source
                    && deferral.hash == hash
                    && (deferral.scope == current || deferral.scope == scope)
                    && deferral.until > at
            })
    }

    fn next_deadline(&mut self, source: u8) -> Option<u64> {
        let now = self.now;
        if self.alarm.is_some_and(|alarm| alarm.until <= now) {
            self.alarm = None;
        }
        self.deferrals.retain(|deferral| deferral.until > now);
        self.alarm
            .map(|alarm| alarm.until)
            .into_iter()
            .chain(
                self.deferrals
                    .iter()
                    .filter(|deferral| deferral.source == source)
                    .map(|deferral| deferral.until),
            )
            .min()
    }
}

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

#[cfg(test)]
fn scope() -> impl Strategy<Value = Scope> {
    (0..GENERATIONS, 0..EPOCHS)
}

#[cfg(test)]
fn retry_op() -> impl Strategy<Value = RetryOp> {
    prop_oneof![
        3 => (prop::collection::btree_set(0..SOURCES, 0..=3), scope(), 0..HASHES, 1..20u64)
            .prop_map(|(sources, scope, hash, ttl)| RetryOp::DeferUnavailable {
                sources: sources.into_iter().collect(),
                scope,
                hash,
                ttl,
            }),
        3 => (0..SOURCES, prop::collection::vec((scope(), 0..HASHES), 0..4), 1..20u64)
            .prop_map(|(source, bodies, ttl)| RetryOp::DeferIncompatible { source, bodies, ttl }),
        1 => prop::option::of((scope(), 0..HASHES, 1..20u64)).prop_map(RetryOp::SetAlarm),
        1 => (scope(), 0..HASHES).prop_map(|(scope, hash)| RetryOp::Clear(scope, hash)),
        1 => prop::option::of(scope()).prop_map(RetryOp::Retain),
        1 => scope().prop_map(RetryOp::Refresh),
        2 => (0..8u64).prop_map(RetryOp::Advance),
        1 => (0..SOURCES).prop_map(RetryOp::NextDeadline),
    ]
}

/// Owners under test, with the base instant that model seconds count from.
struct RetryOwners {
    base: Instant,
    alarms: BodyRetryAlarms,
    suppliers: SupplierRetryState,
}

impl RetryOwners {
    fn at(&self, seconds: u64) -> Instant {
        self.base + Duration::from_secs(seconds)
    }

    fn apply(&mut self, model: &mut RetryModel, op: RetryOp) {
        let now = model.now;
        match op {
            RetryOp::DeferUnavailable {
                sources,
                scope,
                hash: raw_hash,
                ttl,
            } => {
                model.defer_unavailable(&sources, scope, raw_hash, now + ttl);
                let skipped = self.suppliers.defer_unavailable(
                    sources.iter().copied().map(source).collect(),
                    authority(scope),
                    hash(raw_hash),
                    self.at(now + ttl),
                );
                assert_eq!(skipped, 0, "{RETRY_PROPERTY}");
            }
            RetryOp::DeferIncompatible {
                source: raw_source,
                bodies,
                ttl,
            } => {
                model.defer_incompatible(raw_source, &bodies, now + ttl);
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
            }
            RetryOp::SetAlarm(alarm) => {
                model.alarm = alarm.map(|(scope, hash, ttl)| Alarm {
                    scope,
                    hash,
                    until: now + ttl,
                });
                self.alarms.set(alarm.map(|(scope, raw_hash, ttl)| {
                    (authority(scope), hash(raw_hash), self.at(now + ttl))
                }));
            }
            RetryOp::Clear(scope, raw_hash) => {
                model.clear(scope, raw_hash);
                self.suppliers.clear(authority(scope), hash(raw_hash));
                self.alarms.clear(authority(scope), hash(raw_hash));
            }
            RetryOp::Retain(current) => {
                model.retain(current);
                retain_body_retry_scope(
                    &mut self.alarms,
                    &mut self.suppliers,
                    current.map(authority),
                );
                self.check_stale_work_is_retired(current);
            }
            RetryOp::Refresh(current) => {
                model.refresh(current);
                refresh_body_retry_scope(&mut self.alarms, &mut self.suppliers, authority(current));
            }
            RetryOp::Advance(seconds) => model.now += seconds,
            RetryOp::NextDeadline(raw_source) => {
                let expected = model.next_deadline(raw_source).map(|until| self.at(until));
                let all = self.alarms.next_deadline(self.at(now));
                let supplier = self
                    .suppliers
                    .next_deadline(source(raw_source), self.at(now));
                assert_eq!(
                    all.into_iter().chain(supplier).min(),
                    expected,
                    "{RETRY_PROPERTY}"
                );
            }
        }
    }

    /// Right after a retain, work from an older epoch avoids nothing, and a
    /// retain to `None` clears every deferral and alarm.
    fn check_stale_work_is_retired(&self, current: Option<Scope>) {
        for scope in (0..GENERATIONS)
            .flat_map(|generation| (0..EPOCHS).map(move |epoch| (generation, epoch)))
        {
            if current.is_some_and(|current| scope.1 >= current.1) {
                continue;
            }
            for raw_source in 0..SOURCES {
                for raw_hash in 0..HASHES {
                    assert!(
                        !is_body_retry_avoided(
                            &self.alarms,
                            &self.suppliers,
                            source(raw_source),
                            authority(scope),
                            hash(raw_hash),
                            self.base,
                        ),
                        "{RETRY_PROPERTY}: stale scope {scope:?} still avoids after retaining {current:?}");
                }
            }
        }
    }

    /// Compare every lookup with the model now and after a later instant.
    fn check(&self, model: &RetryModel) {
        for at in [model.now, model.now + 7] {
            for raw_source in 0..SOURCES {
                for generation in 0..GENERATIONS {
                    for epoch in 0..EPOCHS {
                        for raw_hash in 0..HASHES {
                            let scope = (generation, epoch);
                            assert_eq!(
                                is_body_retry_avoided(
                                    &self.alarms,
                                    &self.suppliers,
                                    source(raw_source),
                                    authority(scope),
                                    hash(raw_hash),
                                    self.at(at),
                                ),
                                model.is_avoided(raw_source, scope, raw_hash, at),
                                "{RETRY_PROPERTY}: source {raw_source} scope {scope:?} hash {raw_hash} at {at}");
                        }
                    }
                }
            }
        }
    }
}

const RETRY_PROPERTY: &str = "block/retry-authority";

/// Applicable retry operations in a decoder-v1 history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct RetryAuthorityReplaySummary {
    /// Operations compared with the independent model.
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

/// Replay decoder-v1 quadruples against the supplier and alarm model.
/// Read at most 512 bytes. Time is relative to one origin per history.
pub fn replay_retry_authority_bytes(data: &[u8]) -> RetryAuthorityReplaySummary {
    run_retry(decode_retry(data), |_| {})
}

fn decode_retry(data: &[u8]) -> impl Iterator<Item = RetryOp> + '_ {
    data[..data.len().min(512)].chunks(4).map(|chunk| {
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
    let mut model = RetryModel::default();
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
        owners.apply(&mut model, op);
        after_step(&mut owners);
        owners.check(&model);
    }
    summary
}

#[cfg(test)]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn retry_avoidance_matches_an_independent_model(ops in prop::collection::vec(retry_op(), 1..60)) {
        run_retry(ops, |_| {});
    }
    #[test]
    fn retry_bytes_match_the_same_model(bytes in prop::collection::vec(any::<u8>(), 0..=512)) {
        replay_retry_authority_bytes(&bytes);
    }
}

#[cfg(test)]
const RETRY_WITNESS: &[u8] = &[
    0, 0, 3, 2, 1, 0, 3, 3, 5, 1, 0, 0, 4, 4, 0, 0, 2, 4, 0, 2, 6, 0, 0, 5, 7, 0, 0, 0,
];

#[test]
#[allow(
    clippy::print_stdout,
    reason = "CI retains these applicability counters"
)]
fn retry_witness_exercises_expiry_and_authority_changes() {
    let summary = replay_retry_authority_bytes(RETRY_WITNESS);
    assert_eq!(
        summary,
        RetryAuthorityReplaySummary {
            transitions: 7,
            deferrals: 2,
            scope_changes: 2,
            time_advances: 1,
            deadline_queries: 1
        }
    );
    assert_eq!(
        replay_retry_authority_bytes(&[]),
        RetryAuthorityReplaySummary::default()
    );
    println!("tier=Machine target=block_retry_authority decoder=1 {summary:?}");
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

#[test]
fn incompatible_report_from_stale_work_cannot_suppress_current_work() {
    let base = Instant::now();
    let mut alarms = BodyRetryAlarms::default();
    let mut suppliers = SupplierRetryState::default();
    retain_body_retry_scope(&mut alarms, &mut suppliers, Some(authority((0, 1))));
    let until = base + Duration::from_secs(60);
    assert_eq!(
        suppliers.defer_incompatible(source(0), vec![(authority((0, 0)), hash(0))], until, base),
        0,
    );
    assert!(!is_body_retry_avoided(
        &alarms,
        &suppliers,
        source(0),
        authority((0, 1)),
        hash(0),
        base
    ));
    assert!(!is_body_retry_avoided(
        &alarms,
        &suppliers,
        source(0),
        authority((0, 0)),
        hash(0),
        base
    ));
    assert_eq!(suppliers.len(), 0);
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

#[cfg(test)]
fn table_op() -> impl Strategy<Value = TableOp> {
    let heights = || prop::collection::vec(0..HEIGHTS, 0..4);
    prop_oneof![
        2 => (0..PEERS).prop_map(TableOp::Admit),
        3 => (0..PEERS, 0..2u64, 1..4u64, heights(), 0..2u8).prop_map(
            |(peer, age, request, heights, hash)| TableOp::Insert { peer, age, request, heights, hash }
        ),
        2 => (0..PEERS, 0..2u64, 1..4u64, heights()).prop_map(
            |(peer, age, request, heights)| TableOp::RemoveForOwner { peer, age, request, heights }
        ),
        1 => (0..PEERS, 0..2u64).prop_map(|(peer, age)| TableOp::Take { peer, age }),
        2 => (0..PEERS, 0..HEIGHTS, 1..4u64)
            .prop_map(|(peer, height, request)| TableOp::ClearHeight { peer, height, request }),
        1 => (0..PEERS, 0..2u64).prop_map(|(peer, age)| TableOp::Close { peer, age }),
        1 => (0..PEERS).prop_map(TableOp::RemoveClosed),
    ]
}

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
    /// Operations compared with the independent model.
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

/// Replay decoder-v1 quadruples against the existing peer-table model.
/// Read at most 512 bytes and preserve exact generation and owner identities.
pub fn replay_peer_registry_bytes(data: &[u8]) -> PeerRegistryReplaySummary {
    run_table(decode_table(data), |_, _| {})
}

fn decode_table(data: &[u8]) -> impl Iterator<Item = TableOp> + '_ {
    data[..data.len().min(512)].chunks(4).map(|chunk| {
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

#[cfg(test)]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn peer_table_isolates_stale_generations_and_owners(ops in prop::collection::vec(table_op(), 1..60)) {
        run_table(ops, |_, _| {});
    }
    #[test]
    fn peer_table_bytes_match_the_same_model(bytes in prop::collection::vec(any::<u8>(), 0..=512)) {
        replay_peer_registry_bytes(&bytes);
    }
}

#[cfg(test)]
const TABLE_WITNESS: &[u8] = &[
    0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 3, 3, 0, 0, 5, 3, 0, 0, 4, 0, 0, 1, 4, 0, 0, 0,
    5, 0, 0, 0, 6, 0, 0, 0,
];

#[test]
#[allow(
    clippy::print_stdout,
    reason = "CI retains these applicability counters"
)]
fn registry_witness_exercises_replacement_and_exact_owner() {
    let summary = replay_peer_registry_bytes(TABLE_WITNESS);
    assert_eq!(
        summary,
        PeerRegistryReplaySummary {
            transitions: 10,
            admissions: 2,
            replacements: 1,
            stale_generations: 2,
            owner_checks: 2
        }
    );
    assert_eq!(
        replay_peer_registry_bytes(&[]),
        PeerRegistryReplaySummary::default()
    );
    println!("tier=Machine target=block_peer_registry decoder=1 {summary:?}");
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
