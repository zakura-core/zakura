//! Machine tests for [`WorkQueueState`].
//!
//! The proptest drives the pure state with legal input sequences and checks it
//! after every step against an independent model. The model keeps one entry per
//! height and a per-owner charge ledger. It never calls the state's helpers.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    num::NonZeroU64,
};

#[cfg(test)]
use proptest::prelude::*;
use zakura_header_chain::{BodyWorkAuthority, HeaderGeneration, HeaderWorkAuthority};

use super::*;
use crate::zakura::block_sync::test_work_scope;

/// Highest height any generated input touches.
const MAX_HEIGHT: u32 = 20;

fn height(raw: u32) -> block::Height {
    block::Height(raw)
}

fn hash(raw: u32) -> block::Hash {
    let byte = u8::try_from(raw).expect("generated heights stay below 256");
    block::Hash([byte; 32])
}

/// The `generation`th authority after the base test authority.
fn authority(generation: u64) -> BodyWorkAuthority {
    let base = test_work_scope();
    BodyWorkAuthority {
        header: HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(base.header_generation.get() + generation),
            ..base.header
        },
        ..base
    }
}

fn confirmed(size: u64) -> BlockSizeEstimate {
    BlockSizeEstimate::Confirmed(u32::try_from(size).expect("generated sizes fit in u32"))
}

fn new_state() -> WorkQueueState {
    let mut state = WorkQueueState::new(block::Height(0));
    state.set_estimate_floor(1);
    state
}

#[cfg(test)]
fn extend(state: &mut WorkQueueState, scope: BodyWorkAuthority, heights: &[(u32, u64)]) {
    let (_, _) = state.extend(
        scope,
        heights
            .iter()
            .map(|(raw, size)| (height(*raw), hash(*raw), confirmed(*size))),
    );
}

#[cfg(test)]
fn take(
    state: &mut WorkQueueState,
    low: u32,
    high: u32,
    session: u64,
    request: u64,
) -> Vec<(block::Height, WorkItem)> {
    state.take_budgeted(
        height(low),
        height(high),
        usize::MAX,
        u64::MAX,
        Some((
            session,
            NonZeroU64::new(request).expect("tests use nonzero ids"),
        )),
    )
}

#[cfg(test)]
fn publish(state: &mut WorkQueueState, items: &[(block::Height, WorkItem)]) -> u64 {
    let owner = items[0]
        .1
        .owner
        .unwrap_or_else(|| panic!("{PROPERTY}: a request take binds an owner"));
    assert!(state.is_unpublished_claim(owner, items));
    state.publish_claim(items)
}

#[cfg(test)]
fn owner_of(items: &[(block::Height, WorkItem)]) -> BodyWorkOwner {
    items[0]
        .1
        .owner
        .unwrap_or_else(|| panic!("{PROPERTY}: a request take binds an owner"))
}

fn heights_of(items: &[(block::Height, WorkItem)]) -> BTreeSet<block::Height> {
    items.iter().map(|(height, _)| *height).collect()
}

#[test]
fn stale_unpublished_return_after_reset_keeps_the_replacement() {
    let mut state = new_state();
    extend(&mut state, authority(0), &[(1, 100), (2, 100)]);
    let a = take(&mut state, 1, 2, 1, 1);
    assert_eq!(a.len(), 2);

    let reset = state.reset_above(height(0));
    assert_eq!(
        reset.released_bytes, 0,
        "an unpublished claim holds no charge"
    );
    assert!(!reset.wake.revocation);
    extend(&mut state, authority(1), &[(1, 100), (2, 100)]);
    let b = take(&mut state, 1, 2, 2, 1);
    assert_eq!(publish(&mut state, &b), 200);

    state.return_unpublished(&a);
    assert!(!state.is_unpublished_claim(owner_of(&a), &a));
    for (height, _) in &b {
        assert_eq!(state.owner_for_height(*height), Some(owner_of(&b)));
    }
    assert!(state.is_published_claim(owner_of(&b), heights_of(&b)));
    assert_eq!(state.reserved_bytes(), 200);
}

#[test]
fn stale_published_return_after_reset_settles_only_the_old_charge() {
    let mut state = new_state();
    extend(&mut state, authority(0), &[(1, 100), (2, 100)]);
    let a = take(&mut state, 1, 2, 1, 1);
    assert_eq!(publish(&mut state, &a), 200);

    let reset = state.reset_above(height(0));
    assert_eq!(reset.released_bytes, 200, "reset settles A's charge");
    assert!(reset.wake.revocation);
    extend(&mut state, authority(1), &[(1, 100), (2, 100)]);
    let b = take(&mut state, 1, 2, 2, 1);
    assert_eq!(publish(&mut state, &b), 200);

    let (outcome, wake) = state.return_owned(Some(owner_of(&a)), heights_of(&a));
    assert_eq!(
        outcome.released_bytes, 0,
        "A's charge settled once, at reset"
    );
    assert_eq!(outcome.returned_count, 0);
    assert_eq!(outcome.missing_count, 2);
    assert!(!wake.available);
    assert!(state
        .end_reservation_at_receipt(Some(owner_of(&a)), height(1), false)
        .is_none());
    assert!(state.is_published_claim(owner_of(&b), heights_of(&b)));
    assert_eq!(state.reserved_bytes(), 200);
}

#[test]
fn duplicate_settlement_releases_each_charge_once() {
    let mut state = new_state();
    extend(&mut state, authority(0), &[(1, 100), (2, 50)]);
    let a = take(&mut state, 1, 2, 1, 1);
    let owner = owner_of(&a);
    assert_eq!(publish(&mut state, &a), 150);

    let ended = state
        .end_reservation_at_receipt(Some(owner), height(1), false)
        .expect("the owner holds the reservation");
    assert_eq!(ended.released_bytes, 100);
    assert_eq!(ended.owner, Some(owner));
    assert!(state
        .end_reservation_at_receipt(Some(owner), height(1), false)
        .is_none());
    assert!(state
        .claim_received(Some(owner), height(1))
        .is_some_and(|ended| ended.released_bytes == 0));

    let (first, wake) = state.return_owned(Some(owner), heights_of(&a));
    assert_eq!(first.released_bytes, 50);
    assert_eq!(first.returned_count, 1);
    assert_eq!(
        first.released_count, 1,
        "the received height stays in flight"
    );
    assert!(wake.available);
    let (second, wake) = state.return_owned(Some(owner), heights_of(&a));
    assert_eq!(second.released_bytes, 0);
    assert_eq!(second.returned_count, 0);
    assert!(!wake.available);
    assert_eq!(state.reserved_bytes(), 0);
}

#[test]
fn adjacent_takes_by_two_peers_stay_disjoint() {
    let mut state = new_state();
    extend(
        &mut state,
        authority(0),
        &[(1, 100), (2, 100), (3, 100), (4, 100)],
    );
    let a = state.take_budgeted(
        height(1),
        height(4),
        2,
        u64::MAX,
        Some((1, NonZeroU64::MIN)),
    );
    let b = take(&mut state, 1, 4, 2, 1);
    assert_eq!(heights_of(&a), BTreeSet::from([height(1), height(2)]));
    assert_eq!(heights_of(&b), BTreeSet::from([height(3), height(4)]));
    assert_eq!(publish(&mut state, &a), 200);
    assert_eq!(publish(&mut state, &b), 200);

    let (outcome, _) = state.return_owned(Some(owner_of(&a)), heights_of(&a));
    assert_eq!(outcome.released_bytes, 200);
    let (outcome, _) = state.return_owned(Some(owner_of(&a)), heights_of(&b));
    assert_eq!(outcome.released_bytes, 0, "A cannot settle B's charge");
    assert_eq!(outcome.missing_count, 2);
    assert!(state.is_published_claim(owner_of(&b), heights_of(&b)));
    assert_eq!(state.reserved_bytes(), 200);
}

// ---- independent model ----

/// The model's view of one queued height.
#[derive(Copy, Clone, Debug)]
struct ModelEntry {
    in_flight: bool,
    scope: BodyWorkAuthority,
    owner: Option<BodyWorkOwner>,
    provisional: bool,
    estimate: u64,
    /// Reserved bytes this height holds in the ledger.
    charge: u64,
}

/// Bytes each owner moved into the ledger and the bytes settled since.
#[derive(Copy, Clone, Debug, Default)]
struct OwnerLedger {
    charged: u64,
    settled: u64,
}

#[derive(Debug, Default)]
struct Model {
    floor: u32,
    current_authority: Option<BodyWorkAuthority>,
    next_generation: u64,
    entries: BTreeMap<u32, ModelEntry>,
    ledgers: HashMap<BodyWorkOwner, OwnerLedger>,
}

/// Expected counts of a `return_owned` call.
#[derive(Debug, Default, Eq, PartialEq)]
struct ModelReturn {
    released_bytes: u64,
    returned: u64,
    committed: u64,
    already_pending: u64,
    received: u64,
    missing: u64,
}

impl Model {
    fn authority(&self) -> BodyWorkAuthority {
        authority(self.next_generation)
    }

    fn settle(&mut self, entry: ModelEntry) -> u64 {
        if entry.charge == 0 {
            return 0;
        }
        let owner = entry.owner.expect("only an owned height holds a charge");
        let ledger = self
            .ledgers
            .get_mut(&owner)
            .expect("a charge was recorded at publication");
        ledger.settled += entry.charge;
        assert!(
            ledger.settled <= ledger.charged,
            "an owner settled more than it charged"
        );
        entry.charge
    }

    fn extend(&mut self, low: u32, len: u32, estimate: u64) -> usize {
        let scope = self.authority();
        self.current_authority = Some(scope);
        let mut inserted = 0;
        for raw in low..=(low + len).min(MAX_HEIGHT) {
            if raw <= self.floor {
                continue;
            }
            match self.entries.get_mut(&raw) {
                Some(entry) if !entry.in_flight && entry.scope == scope => {
                    entry.estimate = estimate;
                }
                Some(_) => {}
                None => {
                    self.entries.insert(
                        raw,
                        ModelEntry {
                            in_flight: false,
                            scope,
                            owner: None,
                            provisional: false,
                            estimate,
                            charge: 0,
                        },
                    );
                    inserted += 1;
                }
            }
        }
        inserted
    }

    /// Predict and apply a request take: the first contiguous pending run in
    /// `low..=high` under one scope, within the count and byte caps.
    fn take(
        &mut self,
        low: u32,
        high: u32,
        max_count: usize,
        max_bytes: u64,
        owner_ids: (u64, NonZeroU64),
    ) -> Vec<u32> {
        let mut taken = Vec::new();
        let mut bytes = 0u64;
        let mut run_scope = None;
        for (raw, entry) in self.entries.range(low..=high) {
            if entry.in_flight {
                if !taken.is_empty() {
                    break;
                }
                continue;
            }
            if run_scope.is_some_and(|scope| scope != entry.scope) {
                break;
            }
            if taken.last().is_some_and(|last: &u32| last + 1 != *raw) {
                break;
            }
            if !taken.is_empty() && bytes + entry.estimate > max_bytes {
                break;
            }
            taken.push(*raw);
            run_scope = Some(entry.scope);
            bytes += entry.estimate;
            if taken.len() >= max_count {
                break;
            }
        }
        for raw in &taken {
            let entry = self.entries.get_mut(raw).expect("taken from the model");
            let scope = self.current_authority.unwrap_or(entry.scope);
            entry.in_flight = true;
            entry.scope = scope;
            entry.owner = Some(scope.bind(owner_ids.0, owner_ids.1));
            entry.provisional = true;
        }
        taken
    }

    fn is_unpublished(&self, owner: BodyWorkOwner, heights: &[u32]) -> bool {
        !heights.is_empty()
            && heights.iter().all(|raw| {
                self.entries.get(raw).is_some_and(|entry| {
                    entry.in_flight && entry.owner == Some(owner) && entry.provisional
                })
            })
    }

    fn is_published(&self, owner: BodyWorkOwner, heights: &[u32]) -> bool {
        heights.iter().all(|raw| {
            self.entries.get(raw).is_some_and(|entry| {
                entry.in_flight && entry.owner == Some(owner) && entry.charge > 0
            })
        })
    }

    fn publish(&mut self, owner: BodyWorkOwner, heights: &[u32]) -> u64 {
        let mut charged = 0;
        for raw in heights {
            let entry = self.entries.get_mut(raw).expect("checked unpublished");
            entry.provisional = false;
            entry.charge = entry.estimate;
            charged += entry.estimate;
        }
        self.ledgers.entry(owner).or_default().charged += charged;
        charged
    }

    fn return_unpublished(&mut self, owner: BodyWorkOwner, heights: &[u32]) {
        for raw in heights {
            if let Some(entry) = self.entries.get_mut(raw) {
                if entry.in_flight && entry.owner == Some(owner) && entry.provisional {
                    entry.in_flight = false;
                    entry.owner = None;
                    entry.provisional = false;
                }
            }
        }
    }

    fn receive(&mut self, owner: BodyWorkOwner, raw: u32) -> Option<u64> {
        let entry = *self.entries.get(&raw)?;
        if !entry.in_flight || entry.owner != Some(owner) || entry.charge == 0 {
            return None;
        }
        self.entries.insert(raw, ModelEntry { charge: 0, ..entry });
        Some(self.settle(entry))
    }

    fn return_owned(&mut self, owner: BodyWorkOwner, heights: &BTreeSet<u32>) -> ModelReturn {
        let mut expected = ModelReturn::default();
        for raw in heights {
            let Some(entry) = self.entries.get(raw).copied() else {
                expected.missing += 1;
                continue;
            };
            if !entry.in_flight {
                expected.already_pending += 1;
                continue;
            }
            if entry.owner != Some(owner) {
                expected.missing += 1;
                continue;
            }
            if *raw <= self.floor {
                expected.released_bytes += self.settle(entry);
                expected.committed += 1;
                self.entries.remove(raw);
                continue;
            }
            if entry.charge == 0 {
                expected.received += 1;
                continue;
            }
            expected.released_bytes += self.settle(entry);
            expected.returned += 1;
            let entry = self.entries.get_mut(raw).expect("checked above");
            entry.in_flight = false;
            entry.owner = None;
            entry.charge = 0;
        }
        expected
    }

    /// Remove the heights `drop` selects. Returns released bytes and whether
    /// any removed height still held a request reservation.
    fn remove_where(&mut self, drop: impl Fn(u32) -> bool) -> (u64, bool) {
        let removed: Vec<_> = self
            .entries
            .iter()
            .filter(|(raw, _)| drop(**raw))
            .map(|(raw, entry)| (*raw, *entry))
            .collect();
        let mut released = 0;
        let mut revocation = false;
        for (raw, entry) in removed {
            self.entries.remove(&raw);
            revocation |= entry.in_flight && entry.charge > 0;
            released += self.settle(entry);
        }
        (released, revocation)
    }

    fn advance_floor(&mut self, floor: u32) -> (u64, bool) {
        self.floor = self.floor.max(floor);
        let floor = self.floor;
        self.remove_where(|raw| raw <= floor)
    }

    fn reset_above(&mut self, floor: u32) -> (u64, bool) {
        self.floor = floor;
        self.next_generation += 1;
        self.remove_where(|raw| raw > floor)
    }

    fn refresh_authority(&mut self) {
        self.next_generation += 1;
        let scope = self.authority();
        self.current_authority = Some(scope);
        for entry in self.entries.values_mut().filter(|entry| !entry.in_flight) {
            entry.scope = scope;
        }
    }
}

/// One request attempt that a take created.
#[derive(Debug)]
struct Claim {
    owner: BodyWorkOwner,
    raw_heights: Vec<u32>,
    items: Vec<(block::Height, WorkItem)>,
}

#[derive(Clone, Debug)]
enum Op {
    Extend {
        low: u32,
        len: u32,
        size: u64,
    },
    Take {
        session: u64,
        low: u32,
        span: u32,
        max_count: usize,
        max_bytes: u64,
    },
    Publish(usize),
    ReturnUnpublished(usize),
    Receive(usize, usize),
    /// Expiry, timeout, or peer drop of a whole claim.
    Return(usize),
    /// The floor watchdog revokes one height.
    Revoke(usize, usize),
    AdvanceFloor(u32),
    /// Reset above a height, then replace the authority.
    Reset(u32),
    RefreshAuthority,
}

#[cfg(test)]
fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (1..=MAX_HEIGHT, 0..8u32, 1..300u64)
            .prop_map(|(low, len, size)| Op::Extend { low, len, size }),
        4 => (0..3u64, 1..=MAX_HEIGHT, 0..8u32, 1..6usize, 0..600u64).prop_map(
            |(session, low, span, max_count, max_bytes)| Op::Take {
                session,
                low,
                span,
                max_count,
                max_bytes,
            }
        ),
        3 => any::<usize>().prop_map(Op::Publish),
        1 => any::<usize>().prop_map(Op::ReturnUnpublished),
        2 => (any::<usize>(), any::<usize>()).prop_map(|(claim, at)| Op::Receive(claim, at)),
        2 => any::<usize>().prop_map(Op::Return),
        1 => (any::<usize>(), any::<usize>()).prop_map(|(claim, at)| Op::Revoke(claim, at)),
        1 => (0..=MAX_HEIGHT).prop_map(Op::AdvanceFloor),
        1 => (0..=MAX_HEIGHT).prop_map(Op::Reset),
        1 => Just(Op::RefreshAuthority),
    ]
}

/// Compare every observation of the state with the model.
fn check(state: &WorkQueueState, model: &Model) {
    for raw in 0..=MAX_HEIGHT + 1 {
        let pending = state.pending_contains(height(raw));
        let in_flight = state.in_flight_contains(height(raw));
        assert!(
            !(pending && in_flight),
            "{PROPERTY}: height {raw} is in both maps"
        );
        let Some(entry) = model.entries.get(&raw) else {
            assert!(
                !pending && !in_flight,
                "{PROPERTY}: height {raw} should be absent"
            );
            continue;
        };
        assert_eq!(
            in_flight, entry.in_flight,
            "{PROPERTY}: location of height {raw}"
        );
        assert_eq!(
            pending, !entry.in_flight,
            "{PROPERTY}: location of height {raw}"
        );
        let item = state
            .item_for_height(height(raw))
            .unwrap_or_else(|| panic!("{PROPERTY}: a queued height has an item"));
        assert_eq!(item.owner, entry.owner, "{PROPERTY}: owner of height {raw}");
        assert_eq!(
            item.provisional, entry.provisional,
            "{PROPERTY}: publication of {raw}"
        );
        assert_eq!(item.scope, entry.scope, "{PROPERTY}: scope of height {raw}");
        assert_eq!(
            item.estimated_bytes, entry.estimate,
            "{PROPERTY}: estimate of {raw}"
        );
        assert_eq!(
            item.budget.reserved_charge(),
            entry.charge,
            "{PROPERTY}: charge of {raw}"
        );
        assert_eq!(
            state.owner_for_height(height(raw)),
            entry.owner.filter(|_| !entry.provisional),
            "{PROPERTY}: published owner of height {raw}"
        );
    }

    let total: u64 = model.entries.values().map(|entry| entry.charge).sum();
    assert_eq!(state.reserved_bytes(), total, "{PROPERTY}");
    assert_eq!(state.reserved_bytes_scanned(), total, "{PROPERTY}");
    let above: Vec<_> = model
        .entries
        .iter()
        .filter(|(raw, entry)| **raw > model.floor && entry.in_flight && entry.charge > 0)
        .map(|(_, entry)| entry.charge)
        .collect();
    assert_eq!(
        state.reserved_above(height(model.floor)),
        (
            above.iter().sum(),
            u64::try_from(above.len()).expect("the model holds few heights")
        ),
        "{PROPERTY}"
    );

    let mut unsettled = 0;
    for (owner, ledger) in &model.ledgers {
        let outstanding: u64 = model
            .entries
            .values()
            .filter(|entry| entry.owner == Some(*owner))
            .map(|entry| entry.charge)
            .sum();
        assert_eq!(
            ledger.charged,
            ledger.settled + outstanding,
            "{PROPERTY}: owner {owner:?} conserves its charge"
        );
        unsettled += ledger.charged - ledger.settled;
    }
    assert_eq!(state.reserved_bytes(), unsettled, "{PROPERTY}");
}

fn pick<'a>(claims: &'a [Claim], index: &usize) -> Option<&'a Claim> {
    (!claims.is_empty()).then(|| &claims[*index % claims.len()])
}

fn apply(state: &mut WorkQueueState, model: &mut Model, claims: &mut Vec<Claim>, op: Op) {
    match op {
        Op::Extend { low, len, size } => {
            let expected = model.extend(low, len, size);
            let scope = model.authority();
            let (inserted, wake) = state.extend(
                scope,
                (low..=(low + len).min(MAX_HEIGHT))
                    .map(|raw| (height(raw), hash(raw), confirmed(size))),
            );
            assert_eq!(inserted, expected, "{PROPERTY}");
            assert_eq!(wake.available, expected > 0, "{PROPERTY}");
            assert!(!wake.revocation, "{PROPERTY}");
        }
        Op::Take {
            session,
            low,
            span,
            max_count,
            max_bytes,
        } => {
            let high = (low + span).min(MAX_HEIGHT);
            let request = u64::try_from(claims.len() + 1)
                .ok()
                .and_then(NonZeroU64::new)
                .expect("request ids start at one");
            let expected = model.take(low, high, max_count, max_bytes, (session, request));
            let items = state.take_budgeted(
                height(low),
                height(high),
                max_count,
                max_bytes,
                Some((session, request)),
            );
            let taken: Vec<_> = items.iter().map(|(height, _)| height.0).collect();
            assert_eq!(taken, expected, "{PROPERTY}: take selects the model's run");
            if let Some(&(_, item)) = items.first() {
                let owner = item
                    .owner
                    .unwrap_or_else(|| panic!("{PROPERTY}: a request take binds an owner"));
                claims.push(Claim {
                    owner,
                    raw_heights: taken,
                    items,
                });
            }
        }
        Op::Publish(index) => {
            let Some(claim) = pick(claims, &index) else {
                return;
            };
            let current = model.is_unpublished(claim.owner, &claim.raw_heights);
            assert_eq!(
                state.is_unpublished_claim(claim.owner, &claim.items),
                current,
                "{PROPERTY}"
            );
            if current {
                let expected = model.publish(claim.owner, &claim.raw_heights);
                assert_eq!(state.publish_claim(&claim.items), expected, "{PROPERTY}");
            }
            assert_eq!(
                state.is_published_claim(claim.owner, heights_of(&claim.items)),
                model.is_published(claim.owner, &claim.raw_heights),
                "{PROPERTY}"
            );
        }
        Op::ReturnUnpublished(index) => {
            let Some(claim) = pick(claims, &index) else {
                return;
            };
            model.return_unpublished(claim.owner, &claim.raw_heights);
            state.return_unpublished(&claim.items);
        }
        Op::Receive(index, at) => {
            let Some(claim) = pick(claims, &index) else {
                return;
            };
            let raw = claim.raw_heights[at % claim.raw_heights.len()];
            let expected = model.receive(claim.owner, raw);
            let ended = state.end_reservation_at_receipt(Some(claim.owner), height(raw), false);
            assert_eq!(
                ended.map(|ended| ended.released_bytes),
                expected,
                "{PROPERTY}"
            );
            if let Some(ended) = ended {
                assert_eq!(ended.owner, Some(claim.owner), "{PROPERTY}");
            }
        }
        Op::Return(index) => {
            let Some(claim) = pick(claims, &index) else {
                return;
            };
            let heights: BTreeSet<_> = claim.raw_heights.iter().copied().collect();
            return_owned(state, model, claim.owner, heights);
        }
        Op::Revoke(index, at) => {
            let Some(claim) = pick(claims, &index) else {
                return;
            };
            let raw = claim.raw_heights[at % claim.raw_heights.len()];
            return_owned(state, model, claim.owner, BTreeSet::from([raw]));
        }
        Op::AdvanceFloor(floor) => {
            let (released, revocation) = model.advance_floor(floor);
            let delta = state.advance_floor(height(floor));
            assert_eq!(delta.released_bytes, released, "{PROPERTY}");
            assert_eq!(delta.wake.revocation, revocation, "{PROPERTY}");
            assert!(!delta.wake.available, "{PROPERTY}");
        }
        Op::Reset(floor) => {
            let (released, revocation) = model.reset_above(floor);
            let delta = state.reset_above(height(floor));
            assert_eq!(delta.released_bytes, released, "{PROPERTY}");
            assert_eq!(delta.wake.revocation, revocation, "{PROPERTY}");
            assert!(!delta.wake.available, "{PROPERTY}");
        }
        Op::RefreshAuthority => {
            model.refresh_authority();
            let wake = state.refresh_authority(model.authority());
            assert!(wake.available, "{PROPERTY}");
        }
    }
}

fn return_owned(
    state: &mut WorkQueueState,
    model: &mut Model,
    owner: BodyWorkOwner,
    raw_heights: BTreeSet<u32>,
) {
    let expected = model.return_owned(owner, &raw_heights);
    let (outcome, wake) = state.return_owned(
        Some(owner),
        raw_heights.iter().copied().map(height).collect(),
    );
    assert_eq!(
        ModelReturn {
            released_bytes: outcome.released_bytes,
            returned: outcome.returned_count,
            committed: outcome.committed_count,
            already_pending: outcome.already_pending_count,
            received: outcome.released_count,
            missing: outcome.missing_count,
        },
        expected,
        "{PROPERTY}"
    );
    assert_eq!(
        outcome.min_height,
        raw_heights.first().copied().map(height),
        "{PROPERTY}"
    );
    assert_eq!(
        outcome.max_height,
        raw_heights.last().copied().map(height),
        "{PROPERTY}"
    );
    assert_eq!(wake.available, expected.returned > 0, "{PROPERTY}");
    assert!(!wake.revocation, "{PROPERTY}");
}

const PROPERTY: &str = "block/work-ownership";

/// One item read from a completed or interrupted pure transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkOwnershipEntry {
    /// Block height.
    pub height: u32,
    /// Whether the item occupies the in-flight map.
    pub in_flight: bool,
    /// Authority that selected this work.
    pub scope: BodyWorkAuthority,
    /// Exact request owner, including its generation.
    pub owner: Option<BodyWorkOwner>,
    /// Whether publication is still pending.
    pub provisional: bool,
    /// Request estimate retained by this item.
    pub estimate: u64,
    /// Charge retained by this item.
    pub charge: u64,
}

/// The actual owner state or the independent model's expected state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkOwnershipView {
    /// Current download floor.
    pub floor: u32,
    /// Current work authority.
    pub authority: Option<BodyWorkAuthority>,
    /// Items sorted by height and location. Duplicate heights remain visible.
    pub items: Vec<WorkOwnershipEntry>,
    /// Maintained total, or the model's independently summed charges.
    pub reserved: u64,
    /// Charges read from the actual maps, or summed from model entries.
    pub scanned: u64,
    /// Reserved bytes and items above the floor.
    pub above_floor: (u64, u64),
}

/// Where a machine failure occurred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkOwnershipPhase {
    /// Applying an input and checking its immediate outcome.
    Transition,
    /// Checking the resulting state against the independent model.
    Invariant,
}

/// A checked property failure or an unexpected driver panic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkOwnershipFailure {
    /// True only when the independent checker named its property.
    pub property_failure: bool,
    /// The phase that stopped the transition.
    pub phase: WorkOwnershipPhase,
    /// Exact panic payload, without source locations from the panic hook.
    pub message: String,
}

/// Semantic evidence from one operation in a decoder-v1 history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkOwnershipStep {
    /// One-based operation index.
    pub step: usize,
    /// Original decoder bytes, including a short final operation.
    pub input: Vec<u8>,
    /// State read from the real pure owner after the operation or interruption.
    pub actual: WorkOwnershipView,
    /// State derived independently by the model.
    pub expected: WorkOwnershipView,
    /// The failure that interrupted this operation, if any.
    pub failure: Option<WorkOwnershipFailure>,
}

/// A bounded semantic history. A failed history has no successful summary.
#[derive(Debug, Eq, PartialEq)]
pub struct WorkOwnershipHistory {
    /// Every operation reached, including the first failing operation.
    pub steps: Vec<WorkOwnershipStep>,
    /// Present only when the full decoder-v1 input completed.
    pub summary: Option<WorkOwnershipReplaySummary>,
}

fn actual_view(state: &WorkQueueState) -> WorkOwnershipView {
    let mut items: Vec<_> = state
        .pending
        .iter()
        .map(|(height, item)| (height, item, false))
        .chain(
            state
                .in_flight
                .iter()
                .map(|(height, item)| (height, item, true)),
        )
        .map(|(height, item, in_flight)| WorkOwnershipEntry {
            height: height.0,
            in_flight,
            scope: item.scope,
            owner: item.owner,
            provisional: item.provisional,
            estimate: item.estimated_bytes,
            charge: item.budget.reserved_charge(),
        })
        .collect();
    items.sort_by_key(|item| (item.height, item.in_flight));
    WorkOwnershipView {
        floor: state.floor.0,
        authority: state.current_authority,
        items,
        reserved: state.reserved_bytes(),
        scanned: state.reserved_bytes_scanned(),
        above_floor: state.reserved_above(state.floor),
    }
}

fn expected_view(model: &Model) -> WorkOwnershipView {
    let items: Vec<_> = model
        .entries
        .iter()
        .map(|(height, entry)| WorkOwnershipEntry {
            height: *height,
            in_flight: entry.in_flight,
            scope: entry.scope,
            owner: entry.owner,
            provisional: entry.provisional,
            estimate: entry.estimate,
            charge: entry.charge,
        })
        .collect();
    let reserved = items.iter().map(|item| item.charge).sum();
    let above: Vec<_> = items
        .iter()
        .filter(|item| item.height > model.floor && item.in_flight && item.charge > 0)
        .collect();
    WorkOwnershipView {
        floor: model.floor,
        authority: model.current_authority,
        reserved,
        scanned: reserved,
        above_floor: (
            above.iter().map(|item| item.charge).sum(),
            u64::try_from(above.len()).expect("the model holds few heights"),
        ),
        items,
    }
}

/// Capture decoder-v1 operations without changing the decoder or starting a runtime.
/// The history contains at most 128 steps. Driver panics remain distinct from property failures.
pub fn trace_work_ownership_bytes(data: &[u8]) -> WorkOwnershipHistory {
    trace_with(data, |_, _| {})
}

fn trace_with(
    data: &[u8],
    after_step: impl FnMut(&mut WorkQueueState, &Model),
) -> WorkOwnershipHistory {
    let mut steps = Vec::new();
    let input = &data[..data.len().min(512)];
    let mut capture = |state: &WorkQueueState,
                       model: &Model,
                       step: usize,
                       phase: WorkOwnershipPhase,
                       failure: Option<&(dyn std::any::Any + Send)>| {
        let failure = failure.map(|payload| {
            let message = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| {
                    payload
                        .downcast_ref::<&str>()
                        .map(|text| (*text).to_owned())
                })
                .unwrap_or_else(|| "non-string panic payload".to_owned());
            WorkOwnershipFailure {
                property_failure: message == PROPERTY
                    || message.starts_with(&format!("{PROPERTY}:"))
                    || (message.starts_with("assertion")
                        && message.contains(&format!("failed: {PROPERTY}"))),
                phase,
                message,
            }
        });
        steps.push(WorkOwnershipStep {
            step,
            input: input[(step - 1) * 4..(step * 4).min(input.len())].to_vec(),
            actual: actual_view(state),
            expected: expected_view(model),
            failure,
        });
    };
    let summary = run_captured(decode(data), after_step, &mut capture).ok();
    WorkOwnershipHistory { steps, summary }
}

/// Applicable operations and conserved bytes in a decoder-v1 work history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct WorkOwnershipReplaySummary {
    /// Operations checked against the independent ledger.
    pub transitions: usize,
    /// Nonempty takes that created a claim.
    pub claims: usize,
    /// Reset operations checked.
    pub resets: usize,
    /// Returns whose claim no longer owns all its heights.
    pub stale_returns: usize,
    /// Bytes charged by all owners across the history.
    pub charged: u64,
    /// Bytes settled by all owners across the history.
    pub settled: u64,
}

/// Replay decoder-v1 quadruples against the existing ownership ledger.
/// Read at most 512 bytes. The ledger does not inspect production accounting helpers.
pub fn replay_work_ownership_bytes(data: &[u8]) -> WorkOwnershipReplaySummary {
    run(decode(data), |_, _| {})
}

fn decode(data: &[u8]) -> impl Iterator<Item = Op> + '_ {
    data[..data.len().min(512)].chunks(4).map(|chunk| {
        let a = chunk[0];
        let b = chunk.get(1).copied().unwrap_or(0);
        let c = chunk.get(2).copied().unwrap_or(0);
        let d = chunk.get(3).copied().unwrap_or(0);
        match a % 10 {
            0 => Op::Extend {
                low: u32::from(b) % MAX_HEIGHT + 1,
                len: u32::from(c % 8),
                size: u64::from(d) + 1,
            },
            1 => Op::Take {
                session: u64::from(a / 10 % 3),
                low: u32::from(b) % MAX_HEIGHT + 1,
                span: u32::from(c % 8),
                max_count: usize::from(c / 8 % 6) + 1,
                max_bytes: u64::from(d) * 100,
            },
            2 => Op::Publish(usize::from(b)),
            3 => Op::ReturnUnpublished(usize::from(b)),
            4 => Op::Receive(usize::from(b), usize::from(c)),
            5 => Op::Return(usize::from(b)),
            6 => Op::Revoke(usize::from(b), usize::from(c)),
            7 => Op::AdvanceFloor(u32::from(b) % (MAX_HEIGHT + 1)),
            8 => Op::Reset(u32::from(b) % (MAX_HEIGHT + 1)),
            _ => Op::RefreshAuthority,
        }
    })
}

fn run(
    ops: impl IntoIterator<Item = Op>,
    after_step: impl FnMut(&mut WorkQueueState, &Model),
) -> WorkOwnershipReplaySummary {
    match run_captured(ops, after_step, &mut |_, _, _, _, _| {}) {
        Ok(summary) => summary,
        Err(failure) => std::panic::resume_unwind(failure),
    }
}

fn run_captured(
    ops: impl IntoIterator<Item = Op>,
    mut after_step: impl FnMut(&mut WorkQueueState, &Model),
    capture: &mut impl FnMut(
        &WorkQueueState,
        &Model,
        usize,
        WorkOwnershipPhase,
        Option<&(dyn std::any::Any + Send)>,
    ),
) -> Result<WorkOwnershipReplaySummary, Box<dyn std::any::Any + Send>> {
    let mut state = new_state();
    let mut model = Model::default();
    let mut claims = Vec::new();
    let mut summary = WorkOwnershipReplaySummary::default();
    for op in ops {
        summary.transitions += 1;
        summary.resets += usize::from(matches!(op, Op::Reset(_)));
        if let Op::Return(index) = &op {
            if let Some(claim) = pick(&claims, index) {
                summary.stale_returns += usize::from(claim.raw_heights.iter().any(|height| {
                    model
                        .entries
                        .get(height)
                        .is_none_or(|entry| entry.owner != Some(claim.owner))
                }));
            }
        }
        let mut phase = WorkOwnershipPhase::Transition;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            apply(&mut state, &mut model, &mut claims, op);
            phase = WorkOwnershipPhase::Invariant;
            after_step(&mut state, &model);
            check(&state, &model);
        }));
        capture(
            &state,
            &model,
            summary.transitions,
            phase,
            result.as_ref().err().map(|error| &**error),
        );
        result?;
    }
    summary.claims = claims.len();
    summary.charged = model.ledgers.values().map(|ledger| ledger.charged).sum();
    summary.settled = model.ledgers.values().map(|ledger| ledger.settled).sum();
    Ok(summary)
}

#[cfg(test)]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]
    #[test]
    fn work_ownership_and_charges_match_an_independent_ledger(ops in prop::collection::vec(op(), 1..80)) {
        run(ops, |_, _| {});
    }

    #[test]
    fn work_bytes_match_the_same_ledger(bytes in prop::collection::vec(any::<u8>(), 0..=512)) {
        replay_work_ownership_bytes(&bytes);
    }
}

#[cfg(test)]
const WITNESS: &[u8] = &[
    0, 0, 0, 9, 1, 0, 0, 1, 2, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 9, 1, 0, 0, 1, 2, 1, 0, 0, 5, 0, 0, 0,
    4, 1, 0, 0, 5, 1, 0, 0,
];

#[test]
#[allow(
    clippy::print_stdout,
    reason = "witnesses print applicability counters"
)]
fn work_witness_exercises_reset_stale_return_and_settlement() {
    let summary = replay_work_ownership_bytes(WITNESS);
    assert_eq!(
        summary,
        WorkOwnershipReplaySummary {
            transitions: 10,
            claims: 2,
            resets: 1,
            stale_returns: 1,
            charged: 20,
            settled: 20,
        }
    );
    assert_eq!(
        replay_work_ownership_bytes(&[]),
        WorkOwnershipReplaySummary::default()
    );
    println!("tier=Machine target=block_work_ownership decoder=1 {summary:?}");
}

#[test]
#[should_panic(expected = "block/work-ownership")]
fn work_witness_rejects_lost_charge() {
    run(decode(WITNESS), |state, _| {
        state.reserved_bytes = 0;
    });
}

#[test]
#[should_panic(expected = "block/work-ownership")]
fn work_witness_rejects_lost_owner() {
    run(decode(WITNESS), |state, _| {
        for item in state.in_flight.values_mut() {
            item.owner = None;
        }
    });
}

#[test]
fn semantic_history_keeps_inputs_and_independent_states() {
    let history = trace_work_ownership_bytes(WITNESS);
    assert_eq!(history.summary, Some(replay_work_ownership_bytes(WITNESS)));
    assert_eq!(history.steps.len(), 10);
    for (step, input) in history.steps.iter().zip(WITNESS.chunks(4)) {
        assert_eq!(step.input, input);
        assert_eq!(step.actual, step.expected);
        assert!(step.failure.is_none());
    }
    let short = trace_work_ownership_bytes(&[0, 0]);
    assert_eq!(short.steps[0].input, [0, 0]);
    assert_eq!(short.steps[0].actual, short.steps[0].expected);
    let empty = trace_work_ownership_bytes(&[]);
    assert!(empty.steps.is_empty());
    assert_eq!(empty.summary, Some(WorkOwnershipReplaySummary::default()));
}

#[test]
fn semantic_history_retains_the_first_property_failure() {
    let history = trace_with(WITNESS, |state, _| state.reserved_bytes = 0);
    assert!(history.summary.is_none());
    let last = history.steps.last().unwrap();
    let failure = last.failure.as_ref().unwrap();
    assert!(failure.property_failure);
    assert_eq!(failure.phase, WorkOwnershipPhase::Invariant);
    assert!(failure.message.contains(PROPERTY));
    assert_eq!(last.actual.reserved, 0);
    assert!(last.expected.reserved > 0);
    assert!(history.steps[..history.steps.len() - 1]
        .iter()
        .all(|step| step.failure.is_none()));
}

#[test]
fn semantic_history_keeps_driver_failures_distinct() {
    let history = trace_with(WITNESS, |_, _| {
        panic!("driver failed while collecting block/work-ownership")
    });
    assert!(history.summary.is_none());
    assert_eq!(history.steps.len(), 1);
    let failure = history.steps[0].failure.as_ref().unwrap();
    assert!(!failure.property_failure);
    assert_eq!(
        failure.message,
        "driver failed while collecting block/work-ownership"
    );
}

#[test]
#[cfg(debug_assertions)]
fn semantic_capture_runs_when_an_input_panics() {
    let mut captured = false;
    let failure = run_captured(
        [Op::Extend {
            low: u32::MAX,
            len: 1,
            size: 1,
        }],
        |_, _| {},
        &mut |_, _, step, phase, error| {
            assert_eq!(step, 1);
            assert_eq!(phase, WorkOwnershipPhase::Transition);
            assert!(error.is_some());
            captured = true;
        },
    );
    assert!(failure.is_err());
    assert!(captured);
}

mod funding;
pub use funding::{
    trace_work_funding_bytes, FundingReceiptView, FundingRequestPhase, FundingRequestView,
    WorkFundingHistory, WorkFundingStep, WorkFundingView,
};
