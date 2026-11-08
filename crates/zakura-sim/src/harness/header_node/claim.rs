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

use crate::{Outcome, RunEnd};

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
    /// A call was handled with its round's headers still held, or still
    /// held them at teardown.
    OpenOwnership,
    /// A reservation never ended by teardown.
    OpenReservation,
    /// The reactor is gone with headers still reserved.
    TeardownReserved,
    /// The reactor is gone with headers still owned.
    TeardownOwned,
    /// The reactor is gone with a locator permit missing that no
    /// pending storage read holds.
    TeardownLocatorPermits,
    /// A stale prepare or apply's handler had an effect on other work
    /// ([`super::stale`]).
    StaleCompletionEffect,
    /// The reactor's work outlived a reset it adopted, ran under an older
    /// generation, or started from the wrong locator read, or a reset
    /// didn't advance the generation ([`super::round_life`]).
    ResetInvalidatesWork,
    /// A round a new snapshot made obsolete retired while the queue still
    /// held it.
    UnreleasedObsoleteRound,
    /// An apply changed the engine under an older generation or anchor, or
    /// the reactor's adoption moved back ([`super::chain_effect`]).
    StaleChainEffect,
    /// A round's owner isn't bound to its `Open`, a staged page isn't on
    /// its requested path ([`super::authority`]), or the node admitted a
    /// session under another identity than its link's.
    CommitOnlyAuthorized,
    /// A page was staged on a round another session or subscription
    /// requested.
    ForeignPage,
    /// A staged page left its round holding other headers than it carried.
    UnstagedPage,
}

impl Claim {
    /// The tap's name for a ledger claim rule.
    pub(super) fn of(rule: ClaimRule) -> Claim {
        match rule {
            ClaimRule::OverBudget => Claim::OverBudget,
            ClaimRule::LeakedReservation => Claim::LeakedReservation,
            ClaimRule::LeakedLease => Claim::LeakedLease,
            ClaimRule::OpenOwnership => Claim::OpenOwnership,
            ClaimRule::OpenReservation => Claim::OpenReservation,
            ClaimRule::UnreleasedObsoleteRound => Claim::UnreleasedObsoleteRound,
            ClaimRule::ForeignPage => Claim::ForeignPage,
            ClaimRule::UnstagedPage => Claim::UnstagedPage,
        }
    }
}

/// The node's settlement at one drain: its released rounds, and its own
/// teardown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Settlement {
    /// The step whose drain read them.
    pub step: u64,
    /// How many rounds the queue has released so far.
    pub released: u64,
    /// How many of those haven't settled: some of their staged headers
    /// haven't returned, or a call that held their headers hasn't been
    /// handled.
    pub unsettled: usize,
    /// How many of those unsettled rounds have no call that holds their
    /// headers still unended, so storage has nothing left that could settle
    /// them.
    pub stuck: usize,
    /// Whether the harness began stopping the reactor.
    pub shutdown: bool,
    /// Whether the stopped reactor's teardown is done: the reactor is gone
    /// without unwinding, and every call it dropped has its drop receipt.
    pub settled: bool,
}

/// What [`SettlementEvidence`] keeps.
#[derive(Debug, Default)]
struct SettlementStore {
    /// Each drain's settlement, in order, with whether a property read it.
    rows: Vec<(Settlement, bool)>,
    /// Whether the run ended at rest: idle, or with its drain run dry.
    at_rest: bool,
}

/// One reactor's settlement at each drain, each with whether a property
/// read it, and how the run ended, shared by its tap and whoever reads
/// them. Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct SettlementEvidence(Arc<Mutex<SettlementStore>>);

impl SettlementEvidence {
    /// Runs `read` on the store. Nothing else is called while the lock is
    /// held.
    fn with<R>(&self, read: impl FnOnce(&mut SettlementStore) -> R) -> R {
        read(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Keeps `rounds`, unread.
    pub(crate) fn record(&self, rounds: Settlement) {
        self.with(|store| store.rows.push((rounds, false)));
    }

    /// Notes how the run ended: at rest when it went idle or its drain ran
    /// dry, and not when its drain was cut off at its bound.
    pub(crate) fn end(&self, end: &RunEnd) {
        let at_rest = match end.outcome {
            Outcome::Idle { .. } => true,
            Outcome::Budget => end.drain.is_some_and(|drain| !drain.exhausted),
            _ => false,
        };
        self.with(|store| store.at_rest = at_rest);
    }

    /// The settlement the drain of `step` read, if it did, which this marks
    /// read. Only the property that reports it reads it this way.
    pub fn at(&self, step: u64) -> Option<Settlement> {
        self.with(|store| {
            store
                .rows
                .iter_mut()
                .rev()
                .find(|(row, _)| row.step == step)
                .map(|(row, read)| {
                    *read = true;
                    *row
                })
        })
    }

    /// Whether a property read the drain of `step`'s settlement.
    pub(super) fn read_at(&self, step: u64) -> bool {
        self.with(|store| {
            store
                .rows
                .iter()
                .rev()
                .find(|(row, _)| row.step == step)
                .is_some_and(|(_, read)| *read)
        })
    }

    /// Every drain's settlement so far, in order.
    pub fn rows(&self) -> Vec<Settlement> {
        self.with(|store| store.rows.iter().map(|(row, _)| *row).collect())
    }

    /// Whether a drain found the stopped reactor's teardown done.
    pub fn settled(&self) -> bool {
        self.with(|store| store.rows.iter().any(|(row, _)| row.settled))
    }

    /// Whether any drain found a released round.
    pub fn any_released(&self) -> bool {
        self.with(|store| store.rows.iter().any(|(row, _)| row.released > 0))
    }

    /// The last drain's settlement, if the run ended at rest with a
    /// released round unsettled and no call that holds its headers still
    /// pending in storage. Nothing is left that could settle that round
    /// then, whatever storage still holds for others.
    pub fn stuck(&self) -> Option<Settlement> {
        self.with(|store| {
            store
                .rows
                .last()
                .map(|(row, _)| *row)
                .filter(|last| store.at_rest && last.stuck > 0)
        })
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
