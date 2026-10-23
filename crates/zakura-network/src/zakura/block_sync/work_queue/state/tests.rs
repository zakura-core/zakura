//! Machine tests for [`WorkQueueState`].
//!
//! The proptest drives the pure state with legal input sequences and checks it
//! after every step against an independent model. The model keeps one entry per
//! height and a per-owner charge ledger. It never calls the state's helpers.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    num::NonZeroU64,
};

use proptest::{prelude::*, sample::Index};
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

fn extend(state: &mut WorkQueueState, scope: BodyWorkAuthority, heights: &[(u32, u64)]) {
    let (_, _) = state.extend(
        scope,
        heights
            .iter()
            .map(|(raw, size)| (height(*raw), hash(*raw), confirmed(*size))),
    );
}

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

fn publish(state: &mut WorkQueueState, items: &[(block::Height, WorkItem)]) -> u64 {
    let owner = items[0].1.owner.expect("a request take binds an owner");
    assert!(state.is_unpublished_claim(owner, items));
    state.publish_claim(items)
}

fn owner_of(items: &[(block::Height, WorkItem)]) -> BodyWorkOwner {
    items[0].1.owner.expect("a request take binds an owner")
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
    Publish(Index),
    ReturnUnpublished(Index),
    Receive(Index, Index),
    /// Expiry, timeout, or peer drop of a whole claim.
    Return(Index),
    /// The floor watchdog revokes one height.
    Revoke(Index, Index),
    AdvanceFloor(u32),
    /// Reset above a height, then replace the authority.
    Reset(u32),
    RefreshAuthority,
}

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
        3 => any::<Index>().prop_map(Op::Publish),
        1 => any::<Index>().prop_map(Op::ReturnUnpublished),
        2 => (any::<Index>(), any::<Index>()).prop_map(|(claim, at)| Op::Receive(claim, at)),
        2 => any::<Index>().prop_map(Op::Return),
        1 => (any::<Index>(), any::<Index>()).prop_map(|(claim, at)| Op::Revoke(claim, at)),
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
        assert!(!(pending && in_flight), "height {raw} is in both maps");
        let Some(entry) = model.entries.get(&raw) else {
            assert!(!pending && !in_flight, "height {raw} should be absent");
            continue;
        };
        assert_eq!(in_flight, entry.in_flight, "location of height {raw}");
        assert_eq!(pending, !entry.in_flight, "location of height {raw}");
        let item = state
            .item_for_height(height(raw))
            .expect("a queued height has an item");
        assert_eq!(item.owner, entry.owner, "owner of height {raw}");
        assert_eq!(item.provisional, entry.provisional, "publication of {raw}");
        assert_eq!(item.scope, entry.scope, "scope of height {raw}");
        assert_eq!(item.estimated_bytes, entry.estimate, "estimate of {raw}");
        assert_eq!(
            item.budget.reserved_charge(),
            entry.charge,
            "charge of {raw}"
        );
        assert_eq!(
            state.owner_for_height(height(raw)),
            entry.owner.filter(|_| !entry.provisional),
            "published owner of height {raw}",
        );
    }

    let total: u64 = model.entries.values().map(|entry| entry.charge).sum();
    assert_eq!(state.reserved_bytes(), total);
    assert_eq!(state.reserved_bytes_scanned(), total);
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
            "owner {owner:?} conserves its charge",
        );
        unsettled += ledger.charged - ledger.settled;
    }
    assert_eq!(state.reserved_bytes(), unsettled);
}

fn pick<'a>(claims: &'a [Claim], index: &Index) -> Option<&'a Claim> {
    (!claims.is_empty()).then(|| &claims[index.index(claims.len())])
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
            assert_eq!(inserted, expected);
            assert_eq!(wake.available, expected > 0);
            assert!(!wake.revocation);
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
            assert_eq!(taken, expected, "take selects the model's run");
            if let Some(&(_, item)) = items.first() {
                let owner = item.owner.expect("a request take binds an owner");
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
                current
            );
            if current {
                let expected = model.publish(claim.owner, &claim.raw_heights);
                assert_eq!(state.publish_claim(&claim.items), expected);
            }
            assert_eq!(
                state.is_published_claim(claim.owner, heights_of(&claim.items)),
                model.is_published(claim.owner, &claim.raw_heights),
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
            let raw = claim.raw_heights[at.index(claim.raw_heights.len())];
            let expected = model.receive(claim.owner, raw);
            let ended = state.end_reservation_at_receipt(Some(claim.owner), height(raw), false);
            assert_eq!(ended.map(|ended| ended.released_bytes), expected);
            if let Some(ended) = ended {
                assert_eq!(ended.owner, Some(claim.owner));
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
            let raw = claim.raw_heights[at.index(claim.raw_heights.len())];
            return_owned(state, model, claim.owner, BTreeSet::from([raw]));
        }
        Op::AdvanceFloor(floor) => {
            let (released, revocation) = model.advance_floor(floor);
            let delta = state.advance_floor(height(floor));
            assert_eq!(delta.released_bytes, released);
            assert_eq!(delta.wake.revocation, revocation);
            assert!(!delta.wake.available);
        }
        Op::Reset(floor) => {
            let (released, revocation) = model.reset_above(floor);
            let delta = state.reset_above(height(floor));
            assert_eq!(delta.released_bytes, released);
            assert_eq!(delta.wake.revocation, revocation);
            assert!(!delta.wake.available);
        }
        Op::RefreshAuthority => {
            model.refresh_authority();
            let wake = state.refresh_authority(model.authority());
            assert!(wake.available);
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
    );
    assert_eq!(outcome.min_height, raw_heights.first().copied().map(height));
    assert_eq!(outcome.max_height, raw_heights.last().copied().map(height));
    assert_eq!(wake.available, expected.returned > 0);
    assert!(!wake.revocation);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Every height has one location and at most one owner, every owner
    /// settles exactly the bytes it charged, and every wake matches the change.
    #[test]
    fn work_ownership_and_charges_match_an_independent_ledger(
        ops in prop::collection::vec(op(), 1..80),
    ) {
        let mut state = new_state();
        let mut model = Model::default();
        let mut claims = Vec::new();
        for op in ops {
            apply(&mut state, &mut model, &mut claims, op);
            check(&state, &model);
        }
    }
}
