//! The claim rules the reactor tap checks, as view-side evidence.
//!
//! Most rules the tap checks say its evidence is inconsistent, and a break
//! fails the run as a harness failure. A claim rule is one a production bug
//! alone can break and a property's claim names, such as the budget's
//! capacity. The tap records its break here, at the drain that found it,
//! keeps checking, and the owning property reports it at that step.
//!
//! Reading a break through [`ClaimEvidence::at`] marks it reported. A break
//! nothing reported by the next drain, or by the run's end, fails the run,
//! so a harness without the owning property still fails on it.

use std::sync::{Arc, Mutex, PoisonError};

use zakura_network::zakura::observe::ClaimRule;

/// A claim rule the tap checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Claim {
    /// A probe read finds the budget holding more than its capacity.
    BudgetOverCapacity,
    /// A reported reservation takes the budget's claims past its capacity.
    OverBudget,
    /// The queue released a round with its peer still holding a
    /// reservation.
    LeakedReservation,
    /// A released round kept a lease past its settlement.
    LeakedLease,
    /// A call was handled with its round's headers still held.
    OpenOwnership,
}

impl Claim {
    /// The tap's name for a ledger claim rule.
    pub(super) fn of(rule: ClaimRule) -> Claim {
        match rule {
            ClaimRule::OverBudget => Claim::OverBudget,
            ClaimRule::LeakedReservation => Claim::LeakedReservation,
            ClaimRule::LeakedLease => Claim::LeakedLease,
            ClaimRule::OpenOwnership => Claim::OpenOwnership,
        }
    }
}

/// The node's released rounds at one drain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rounds {
    /// The step whose drain read them.
    pub step: u64,
    /// How many rounds the queue has released so far.
    pub released: u64,
    /// How many of those haven't settled: some of their staged headers
    /// haven't returned.
    pub unsettled: usize,
}

/// One reactor's released rounds at each drain, shared by its tap and
/// whoever reads them. Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct RoundEvidence(Arc<Mutex<Vec<Rounds>>>);

impl RoundEvidence {
    /// Keeps `rounds`. Nothing else is called while the lock is held.
    pub(crate) fn record(&self, rounds: Rounds) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(rounds);
    }

    /// The rounds the drain of `step` read, if it did.
    pub fn at(&self, step: u64) -> Option<Rounds> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .rev()
            .find(|rounds| rounds.step == step)
            .copied()
    }

    /// Every drain's rounds so far, in order.
    pub fn rows(&self) -> Vec<Rounds> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Whether any drain found a released round.
    pub fn any_released(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|rounds| rounds.released > 0)
    }
}

/// One claim break the tap found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimRecord {
    /// The step whose drain found it.
    pub step: u64,
    /// The rule it broke.
    pub claim: Claim,
    /// What broke it, led by the rule's name.
    pub detail: String,
}

/// One reactor's claim breaks, each with whether a property has read it,
/// shared by its tap and whoever reads them. Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct ClaimEvidence(Arc<Mutex<Vec<(ClaimRecord, bool)>>>);

impl ClaimEvidence {
    /// Keeps `record`, unreported. Nothing else is called while the lock is
    /// held.
    pub(crate) fn record(&self, record: ClaimRecord) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((record, false));
    }

    /// The breaks of `claims` that step `step`'s drain found, in order, which
    /// this marks reported. Only the property that reports them reads them
    /// this way.
    pub fn at(&self, step: u64, claims: &[Claim]) -> Vec<ClaimRecord> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter_mut()
            .filter(|(record, _)| record.step == step && claims.contains(&record.claim))
            .map(|(record, reported)| {
                *reported = true;
                record.clone()
            })
            .collect()
    }

    /// The first break no property reported, among those found before step
    /// `before`, or among all with `None`.
    pub(super) fn unreported(&self, before: Option<u64>) -> Option<ClaimRecord> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|(record, reported)| !reported && before.is_none_or(|step| record.step < step))
            .map(|(record, _)| record.clone())
    }

    /// Every break so far, in order.
    pub fn records(&self) -> Vec<ClaimRecord> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(record, _)| record.clone())
            .collect()
    }
}
