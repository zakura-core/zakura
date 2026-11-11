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

use crate::RunEnd;

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
    /// One of the reactor's growable containers grew past its bound, or
    /// kept a key that no longer has what its entry is for
    /// ([`super::outstanding`]), or the work books didn't balance: a
    /// reservation, round or call that held headers broke the ledger's
    /// accounting ([`super::tap`]'s `WORK_BOOKS`), or a round retired
    /// unreleased for a reason no other property owns.
    OutstandingWork,
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
    /// A page spent more headers than its reservation held.
    OverReservation,
    /// A call held a round's headers for another owner than its pages'.
    WrongOwner,
    /// A prepare sent other headers than its round's staged ones.
    WrongPayload,
    /// A prepare handed its headers to an apply for other work.
    BadHandoff,
    /// A call held or ended another round than it should.
    WrongRound,
    /// A call that isn't the prepare or apply it names held headers.
    WrongCall,
    /// A call held headers with another round's lease.
    ForeignLease,
    /// A round's calls held more headers than it owns.
    HeldUnowned,
    /// A call held a round that staged no page.
    UnstagedHeaders,
    /// A prepare supplied its round from another source than its peer's.
    WrongSource,
    /// A slot outlived its departed session's unit of work, or work started
    /// or applied for a departed session ([`super::retirement`]).
    PeerRetirementProgress,
    /// A round retired for its session's departure or its timeout while
    /// the queue still held it.
    UnreleasedDepartedRound,
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
            ClaimRule::UnreleasedRound => Claim::OutstandingWork,
            ClaimRule::UnreleasedDepartedRound => Claim::UnreleasedDepartedRound,
            ClaimRule::ForeignPage => Claim::ForeignPage,
            ClaimRule::UnstagedPage => Claim::UnstagedPage,
            ClaimRule::OverReservation => Claim::OverReservation,
            ClaimRule::WrongOwner => Claim::WrongOwner,
            ClaimRule::WrongPayload => Claim::WrongPayload,
            ClaimRule::BadHandoff => Claim::BadHandoff,
            ClaimRule::WrongRound => Claim::WrongRound,
            ClaimRule::WrongCall => Claim::WrongCall,
            ClaimRule::ForeignLease => Claim::ForeignLease,
            ClaimRule::HeldUnowned => Claim::HeldUnowned,
            ClaimRule::UnstagedHeaders => Claim::UnstagedHeaders,
            ClaimRule::WrongSource => Claim::WrongSource,
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
    /// How many released rounds a claim tainted. Such a round never
    /// settles, and its teardown isn't judged.
    pub tainted: usize,
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

    /// Notes how the run ended: at rest when it ran dry at rest
    /// ([`RunEnd::at_rest`]), and not at a violation, a harness failure, a
    /// drain cut off at its bound included, or a dry end worker-starved or
    /// deadlocked.
    pub(crate) fn end(&self, end: &RunEnd) {
        let at_rest = end.at_rest();
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

    /// Whether step `step`'s drain found a break.
    pub(super) fn found_at(&self, step: u64) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|(record, _)| record.step == step)
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
