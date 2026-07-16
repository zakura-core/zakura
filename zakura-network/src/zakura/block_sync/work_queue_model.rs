//! Model-based transition test over [`WorkQueue`] + [`RetainedBodyMemoryTracker`].
//!
//! The fuzz scenarios assert accounting only at teardown, which is structurally blind
//! to a leak that a later `advance_floor` sweeps up: the end state is clean even though
//! an intermediate state was not. This test asserts the conservation laws after *every*
//! generated operation, so a charge that is briefly stranded fails immediately, at the
//! transition that stranded it.
//!
//! The reference model tracks accounting, not scheduling: which heights
//! [`take_in_range_budgeted`](WorkQueue::take_in_range_budgeted) selects is driven from
//! the call's own result rather than re-derived here. Its contiguity and byte-cap rules
//! already have dedicated unit tests (`work_queue_budgeted_take_*`), and reimplementing
//! them in the model would only test the copy. What the model does own is every byte:
//! which heights hold a request reservation, which hold a retained-memory reservation,
//! and what the authoritative tracker total must therefore be.

use std::{collections::BTreeMap, sync::Arc};

use proptest::prelude::*;
use zakura_chain::block;

use super::{
    request::BlockSizeEstimate,
    retained_memory::{RetainedBodyMemoryTracker, RetainedCharge},
    work_queue::{UnmatchedBodyClaimOutcome, WorkQueue},
};

/// Height universe. Small on purpose: operations must collide on the same heights
/// often enough to exercise re-issue, late receipt, and reset interleavings.
const MAX_HEIGHT: u32 = 12;

/// Wide enough that a reservation is never refused, so the test exercises accounting
/// rather than admission (which `retained_memory`'s own tests cover).
const MEMORY_LIMIT: u64 = u64::MAX;

/// Where a height sits in the queue. Below-floor heights are absent from the model.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Location {
    Pending,
    InFlight,
}

#[derive(Copy, Clone, Debug)]
struct ModelItem {
    estimated_bytes: u64,
    location: Location,
    /// Whether the request-byte ledger is `Reserved(estimated_bytes)`.
    reserved: bool,
    /// Bytes of the retained-memory reservation the queue holds for this height.
    reservation_bytes: Option<u64>,
}

/// The expected accounting state, maintained independently of the implementation.
#[derive(Debug, Default)]
struct Model {
    floor: u32,
    items: BTreeMap<u32, ModelItem>,
    /// Heights exclusively owned by an outstanding unmatched-body claim, and the token
    /// that owns each. Mirrors the queue's `received_claims`.
    claims: BTreeMap<u32, u64>,
    next_token: u64,
}

impl Model {
    /// Sum of request-byte reservations across pending + in-flight.
    fn reserved_bytes(&self) -> u64 {
        self.items
            .values()
            .filter(|item| item.reserved)
            .fold(0, |acc, item| acc.saturating_add(item.estimated_bytes))
    }

    /// Bytes and count of retained-memory reservations the queue still holds.
    fn reservations(&self) -> (u64, u64) {
        self.items
            .values()
            .filter_map(|item| item.reservation_bytes)
            .fold((0, 0), |(bytes, count), reservation| {
                (bytes.saturating_add(reservation), count + 1)
            })
    }

    fn extend(&mut self, height: u32, estimated_bytes: u64) {
        if height <= self.floor || self.items.contains_key(&height) {
            return;
        }
        self.items.insert(
            height,
            ModelItem {
                estimated_bytes,
                location: Location::Pending,
                reserved: false,
                reservation_bytes: None,
            },
        );
    }

    fn advance_floor(&mut self, floor: u32) {
        self.floor = self.floor.max(floor);
        self.items.retain(|height, _| *height > self.floor);
        self.claims.retain(|height, _| *height > self.floor);
    }

    /// A frontier reset pins the floor rather than raising it, and drops above it.
    fn reset_above(&mut self, floor: u32) {
        self.floor = floor;
        self.items.retain(|height, _| *height <= self.floor);
        self.claims.retain(|height, _| *height <= self.floor);
    }

    /// Mirror `claim_unmatched_body`, returning the claim token on success.
    ///
    /// The generated hash always matches the stored one, so the hash-mismatch arm of
    /// the implementation is not modelled here; `work_queue` unit tests cover it.
    fn claim_unmatched(&mut self, height: u32) -> Option<u64> {
        if self.claims.contains_key(&height) {
            return None;
        }
        let item = self.items.get_mut(&height)?;
        match item.location {
            Location::Pending => item.location = Location::InFlight,
            // An in-flight height whose request reservation is already released is not
            // claimable: no outstanding request owns it.
            Location::InFlight if !item.reserved => return None,
            Location::InFlight => {}
        }
        item.reserved = false;
        // The retained-memory reservation transfers out of the queue to the claimant.
        item.reservation_bytes = None;

        let token = self.next_token;
        self.next_token += 1;
        self.claims.insert(height, token);
        Some(token)
    }

    /// Mirror `rollback_unmatched_claim`: an uncommitted claim returns its height.
    fn rollback_claim(&mut self, height: u32, token: u64) {
        if self.claims.get(&height) != Some(&token) {
            return;
        }
        self.claims.remove(&height);
        let Some(item) = self.items.get_mut(&height) else {
            return;
        };
        if item.location != Location::InFlight {
            return;
        }
        // The queue pops the item out of `in_flight` before re-checking the floor, so a
        // height that slipped below it is dropped rather than returned to `pending`.
        if height <= self.floor {
            self.items.remove(&height);
            return;
        }
        item.location = Location::Pending;
    }
}

/// One generated operation.
#[derive(Clone, Debug)]
enum Op {
    Extend {
        height: u32,
        size: u32,
    },
    TakeAndIssue {
        low: u32,
        high: u32,
        max_count: u32,
    },
    Receive {
        height: u32,
        exact_bytes: u64,
    },
    /// A late/unmatched response racing for a height it was not issued.
    ClaimUnmatched {
        height: u32,
        exact_bytes: u64,
        /// Whether the claimant accepts the body (`commit`) or abandons it, which must
        /// roll the height back and release the transferred reservation.
        commit: bool,
    },
    Return {
        heights: Vec<u32>,
    },
    AdvanceFloor {
        height: u32,
    },
    ResetAbove {
        height: u32,
    },
    DropBody {
        height: u32,
    },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Weighted toward supply + issue so the later ops have state to act on.
        3 => (1u32..=MAX_HEIGHT, 1024u32..8192).prop_map(|(height, size)| Op::Extend { height, size }),
        3 => (1u32..=MAX_HEIGHT, 1u32..=MAX_HEIGHT, 1u32..4)
            .prop_map(|(a, b, max_count)| Op::TakeAndIssue {
                low: a.min(b),
                high: a.max(b),
                max_count,
            }),
        3 => (1u32..=MAX_HEIGHT, 512u64..16_384)
            .prop_map(|(height, exact_bytes)| Op::Receive { height, exact_bytes }),
        3 => (1u32..=MAX_HEIGHT, 512u64..16_384, any::<bool>())
            .prop_map(|(height, exact_bytes, commit)| Op::ClaimUnmatched {
                height,
                exact_bytes,
                commit,
            }),
        2 => proptest::collection::vec(1u32..=MAX_HEIGHT, 0..4)
            .prop_map(|heights| Op::Return { heights }),
        1 => (0u32..=MAX_HEIGHT).prop_map(|height| Op::AdvanceFloor { height }),
        1 => (0u32..=MAX_HEIGHT).prop_map(|height| Op::ResetAbove { height }),
        2 => (1u32..=MAX_HEIGHT).prop_map(|height| Op::DropBody { height }),
    ]
}

/// Drive one operation against both the implementation and the model.
///
/// `bodies` holds each received body's charge alongside the size the model expects it
/// to hold. The test itself is the downstream pipeline owner, standing in for the
/// reorder/applying stages.
///
/// The expected size is tracked here rather than read back via the charge, so the
/// conservation assertion compares the tracker against an independently-derived number.
/// Reading the size out of the charge under test would make the law circular: it would
/// hold no matter what size was actually charged.
fn apply_op(
    op: &Op,
    queue: &Arc<WorkQueue>,
    memory: &RetainedBodyMemoryTracker,
    model: &mut Model,
    bodies: &mut BTreeMap<u32, (RetainedCharge, u64)>,
) {
    match *op {
        Op::Extend { height, size } => {
            queue.extend([(
                block::Height(height),
                block::Hash([height as u8; 32]),
                BlockSizeEstimate::Confirmed(size),
            )]);
            model.extend(height, u64::from(size));
        }

        Op::TakeAndIssue {
            low,
            high,
            max_count,
        } => {
            let taken = queue.take_in_range_budgeted(
                block::Height(low),
                block::Height(high),
                max_count as usize,
                u64::MAX,
            );
            if taken.is_empty() {
                return;
            }

            // Reserve retained memory for the whole request, all or none, exactly as the
            // peer routine does at issue.
            let estimates: Vec<u64> = taken.iter().map(|(_, item)| item.estimated_bytes).collect();
            let reservations = memory
                .try_reserve_many(&estimates)
                .expect("an unbounded limit always admits a reservation");

            let issued = queue.mark_issued(
                taken
                    .iter()
                    .zip(reservations)
                    .map(|((height, _), reservation)| (*height, Some(reservation))),
            );

            // The take moved these heights `pending -> in_flight` whether or not the
            // issue was accepted, so mirror that first.
            for (height, item) in &taken {
                if let Some(entry) = model.items.get_mut(&height.0) {
                    entry.location = Location::InFlight;
                    entry.estimated_bytes = item.estimated_bytes;
                }
            }
            if issued.is_some() {
                for (height, item) in &taken {
                    if let Some(entry) = model.items.get_mut(&height.0) {
                        entry.reserved = true;
                        entry.reservation_bytes = Some(item.estimated_bytes);
                    }
                }
            }
        }

        Op::Receive {
            height,
            exact_bytes,
        } => {
            let settled = queue.claim_received(block::Height(height));

            // A received height is in-flight with its request reservation released; any
            // retained-memory reservation transfers out to the receiving owner.
            if let Some(entry) = model.items.get_mut(&height) {
                entry.location = Location::InFlight;
                entry.reserved = false;
                entry.reservation_bytes = None;
            }

            if let Some(reservation) = settled.in_flight_memory_reservation {
                // Reconciling to the exact size must move the charge, not recreate it:
                // the tracker total is asserted against the model right after.
                let charge = reservation.reconcile_exact(exact_bytes);
                // A duplicate response for a height whose body we already hold replaces
                // the old charge; dropping it here is the real owner's drop.
                bodies.insert(height, (charge, exact_bytes));
            }
        }

        Op::ClaimUnmatched {
            height,
            exact_bytes,
            commit,
        } => {
            let outcome =
                queue.claim_unmatched_body(block::Height(height), block::Hash([height as u8; 32]));
            let expected = model.claim_unmatched(height);

            match outcome {
                UnmatchedBodyClaimOutcome::Claimed(mut claim) => {
                    let token = expected.expect("the model must also grant the claim");

                    if commit {
                        // The accepting path: take the transferred reservation, reconcile
                        // it to the body's exact size, and keep the height claimed.
                        if let Some(reservation) = claim.take_memory_reservation() {
                            bodies.insert(
                                height,
                                (reservation.reconcile_exact(exact_bytes), exact_bytes),
                            );
                        }
                        claim.commit();
                    } else {
                        // The abandoning path: dropping the claim must roll the height
                        // back and release the reservation it carried.
                        drop(claim);
                        model.rollback_claim(height, token);
                    }
                }
                UnmatchedBodyClaimOutcome::AlreadyClaimed
                | UnmatchedBodyClaimOutcome::NotWanted => {
                    assert!(
                        expected.is_none(),
                        "the queue refused a claim for height {height} that the model granted",
                    );
                }
            }
        }

        Op::Return { ref heights } => {
            let outcome = queue.release_reserved_and_return_items_detailed(
                heights.iter().copied().map(block::Height),
            );

            let mut released = 0u64;
            for height in heights {
                let Some(entry) = model.items.get_mut(height) else {
                    continue;
                };
                // Only still-reserved in-flight heights are returned; a received height
                // (ledger already released) stays put.
                if entry.location != Location::InFlight || !entry.reserved {
                    continue;
                }
                released = released.saturating_add(entry.estimated_bytes);
                entry.location = Location::Pending;
                entry.reserved = false;
                entry.reservation_bytes = None;
            }
            assert_eq!(
                outcome.released_bytes, released,
                "returned request bytes disagreed with the model",
            );
        }

        Op::AdvanceFloor { height } => {
            queue.advance_floor(block::Height(height));
            model.advance_floor(height);
        }

        Op::ResetAbove { height } => {
            queue.reset_above(block::Height(height));
            model.reset_above(height);
        }

        Op::DropBody { height } => {
            bodies.remove(&height);
        }
    }
}

/// Assert every conservation law that must hold after each operation.
fn assert_conserved(
    queue: &Arc<WorkQueue>,
    memory: &RetainedBodyMemoryTracker,
    model: &Model,
    bodies: &BTreeMap<u32, (RetainedCharge, u64)>,
    step: usize,
    op: &Op,
) -> Result<(), TestCaseError> {
    let context = format!("after step {step} ({op:?})");

    prop_assert_eq!(
        queue.reserved_bytes(),
        queue.reserved_bytes_scanned(),
        "{}: the maintained reserved counter drifted from its scanned ground truth",
        context
    );
    prop_assert_eq!(
        queue.reserved_bytes(),
        model.reserved_bytes(),
        "{}: reserved request bytes disagreed with the model",
        context
    );

    let (expected_bytes, expected_count) = model.reservations();
    prop_assert_eq!(
        queue.in_flight_memory_reservations_for_test(),
        (expected_bytes, expected_count),
        "{}: in-flight memory reservations disagreed with the model",
        context
    );

    // The conservation law: the authoritative total is exactly the reservations the
    // queue still holds plus the live body charges the pipeline owns. A charge stranded
    // by any transition — released twice, or leaked past its last owner — breaks this
    // the moment it happens, rather than at teardown where a floor sweep could hide it.
    let live_bodies: u64 = bodies.values().map(|(_, expected)| *expected).sum();
    prop_assert_eq!(
        memory.used(),
        expected_bytes.saturating_add(live_bodies),
        "{}: retained total is not reservations ({}) + live bodies ({})",
        context,
        expected_bytes,
        live_bodies
    );

    Ok(())
}

proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(256))]

    #[test]
    fn work_queue_and_retained_memory_conserve_every_transition(
        ops in proptest::collection::vec(op_strategy(), 1..40),
    ) {
        let queue = Arc::new(WorkQueue::new(block::Height(0)));
        let memory = RetainedBodyMemoryTracker::new(MEMORY_LIMIT);
        let mut model = Model::default();
        let mut bodies: BTreeMap<u32, (RetainedCharge, u64)> = BTreeMap::new();

        for (step, op) in ops.iter().enumerate() {
            apply_op(op, &queue, &memory, &mut model, &mut bodies);
            assert_conserved(&queue, &memory, &model, &bodies, step, op)?;
        }

        // Teardown: once the queue and every body owner are gone, nothing may remain
        // charged. This is the same zero-slack statement the fuzz harness makes, but
        // reached through an arbitrary generated history rather than one happy path.
        drop(queue);
        bodies.clear();
        prop_assert_eq!(
            memory.used(),
            0,
            "retained memory did not drain to zero after dropping every owner"
        );
    }
}
