//! The node under test's header capacity, read by its resource probe and
//! checked against the work queue's accounting.
//!
//! The reactor hands its tap a [`HeaderResourceProbe`] as it's built. At
//! each boundary the tap reads it inside a
//! [`boundary_read`](zakura_rt::sim::boundary_read), after it drains, and
//! checks the read with [`check`]:
//!
//! - `BudgetOverCapacity`: the budget never holds more than its capacity
//!   reserved and owned together. Every read checks it. It's a claim rule
//!   ([`super::claim`]), so the tap records its break and goes on.
//! - `ReservedMismatch` and `OwnedMismatch`: at a quiet boundary, with the
//!   reactor alive, the budget holds exactly what the tap's
//!   [`HeaderWorkLedger`] says the queue holds. A boundary is quiet when the
//!   reactor's task, which the probe names, isn't stopped mid-poll. Only
//!   that task changes the budget, and the event reporting each change
//!   follows it in the same poll, so elsewhere the read isn't compared.
//!   Accounting reported from another task fails as `AccountingOffReactor`,
//!   since then no boundary is known quiet.
//! - `TeardownReserved`, `TeardownOwned` and `TeardownLocatorPermits`: once
//!   a reactor the harness stopped is gone, nothing stays reserved or owned,
//!   and every locator permit is free except one for each locator read
//!   storage still holds pending. They're claim rules too, checked on every
//!   read after the reactor is gone against what storage holds then, and
//!   each reported once. The probe holds the locator semaphore, so
//!   a permit forgotten without returning still counts.
//!
//! These read counters that no owner event reports, so they catch a stray
//! reservation, lease or permit that every event leaves clean. A read that
//! finds the budget's lock held is skipped, never counted as zero.
//!
//! The reads stay out of the observation log, so pinned Plans and traces
//! keep their format. A [`CapacityEvidence`] keeps each one whose values
//! differ from the last it kept, and how the read went at each boundary
//! ([`BoundaryRead`]), so a check can tell an unchanged read from none.
//!
//! [`HeaderWorkLedger`]: zakura_network::zakura::observe::HeaderWorkLedger

use std::sync::{Arc, Mutex, PoisonError};

use zakura_network::zakura::observe::{HeaderResourceProbe, HeaderResources};

use super::claim::Claim;

/// One probe read the tap checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CapacityReading {
    /// The step whose boundary it was read at, 0 for setup.
    pub step: u64,
    /// Headers reserved.
    pub reserved: usize,
    /// Headers owned.
    pub owned: usize,
    /// Free locator permits.
    pub locator_permits: usize,
    /// Whether it was compared with the queue's accounting.
    pub compared: bool,
    /// Whether it was a teardown read, after a stopped reactor was gone.
    pub teardown: bool,
}

impl CapacityReading {
    /// Whether `other` holds the same values, whatever its step.
    fn same_values(&self, other: &CapacityReading) -> bool {
        CapacityReading { step: 0, ..*self } == CapacityReading { step: 0, ..*other }
    }
}

/// How the tap's probe read went at one boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoundaryRead {
    /// The probe was read.
    Read,
    /// The budget's lock was held, with the reactor's task stopped mid-poll,
    /// as it is inside a release.
    UnreadMidPoll,
    /// The budget's lock was held, with the reactor's task not stopped
    /// mid-poll, so no task should have held it.
    UnreadQuiet,
}

/// What [`CapacityEvidence`] keeps.
#[derive(Debug, Default)]
struct Store {
    /// The readings, each differing from the one before.
    readings: Vec<CapacityReading>,
    /// Each boundary the tap read at, by step, in order.
    boundaries: Vec<(u64, BoundaryRead)>,
}

/// One reactor's probe reads, shared by its tap and whoever reads them.
/// Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct CapacityEvidence(Arc<Mutex<Store>>);

impl CapacityEvidence {
    /// Keeps `reading` unless it holds the last kept reading's values, and
    /// notes its boundary as read. Nothing else is called while the lock is
    /// held.
    pub(crate) fn record(&self, reading: CapacityReading) {
        let mut store = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        store.boundaries.push((reading.step, BoundaryRead::Read));
        if store
            .readings
            .last()
            .is_none_or(|last| !last.same_values(&reading))
        {
            store.readings.push(reading);
        }
    }

    /// Notes that the boundary after `step` went unread, as `read` says.
    pub(crate) fn unread(&self, step: u64, read: BoundaryRead) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .boundaries
            .push((step, read));
    }

    /// Every reading kept so far, in order.
    pub fn readings(&self) -> Vec<CapacityReading> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .readings
            .clone()
    }

    /// How the read at the boundary after `step` went, or `None` if the tap
    /// didn't try one there.
    pub fn boundary(&self, step: u64) -> Option<BoundaryRead> {
        let store = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        store
            .boundaries
            .iter()
            .rev()
            .find(|(at, _)| *at == step)
            .map(|(_, read)| *read)
    }

    /// The reading kept at the boundary after `step`, if one was.
    pub fn reading_at(&self, step: u64) -> Option<CapacityReading> {
        let store = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        store
            .readings
            .iter()
            .rev()
            .find(|reading| reading.step == step)
            .copied()
    }
}

/// A rule a read breaks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Broken {
    /// A claim rule, which a property reports.
    Claim(Claim, String),
    /// An integrity rule, which fails the run.
    Integrity(String),
}

/// What a read is checked against.
#[derive(Clone, Copy, Debug)]
pub(super) enum Expected {
    /// The reactor is alive. At a quiet boundary, the queue's accounting:
    /// its reserved and owned headers.
    Live {
        /// The accounting, if the boundary is quiet.
        accounted: Option<(usize, usize)>,
    },
    /// The harness stopped the reactor and it's gone. Storage still holds
    /// `storage_permits` locator permits for its pending reads.
    Teardown {
        /// Locator permits storage holds.
        storage_permits: usize,
    },
}

/// Checks `read` from `probe` against `expected`, naming the first rule it
/// breaks, as the module doc lists them. A claim break skips the read's
/// remaining comparisons.
pub(super) fn check(
    probe: &HeaderResourceProbe,
    read: HeaderResources,
    expected: Expected,
) -> Result<(), Broken> {
    let claimed = read.reserved.saturating_add(read.owned);
    if claimed > probe.capacity() {
        return Err(Broken::Claim(
            Claim::BudgetOverCapacity,
            format!(
                "BudgetOverCapacity: the header budget holds {} reserved and {} owned, over \
                 its {} headers",
                read.reserved,
                read.owned,
                probe.capacity()
            ),
        ));
    }
    check_accounting(probe, read, expected)
}

/// The rest of [`check`]: the read against the queue's accounting, which
/// integrity rules check, or the teardown baseline, which claim rules
/// check.
fn check_accounting(
    probe: &HeaderResourceProbe,
    read: HeaderResources,
    expected: Expected,
) -> Result<(), Broken> {
    match expected {
        Expected::Live { accounted: None } => Ok(()),
        Expected::Live {
            accounted: Some((reserved, owned)),
        } => {
            if read.reserved != reserved {
                Err(Broken::Integrity(format!(
                    "ReservedMismatch: at a quiet boundary the header budget holds {} \
                     reserved, and the queue's accounting {reserved}",
                    read.reserved
                )))
            } else if read.owned != owned {
                Err(Broken::Integrity(format!(
                    "OwnedMismatch: at a quiet boundary the header budget holds {} owned, \
                     and the queue's accounting {owned}",
                    read.owned
                )))
            } else {
                Ok(())
            }
        }
        Expected::Teardown { storage_permits } => {
            let free = probe.locator_capacity().saturating_sub(storage_permits);
            if read.reserved != 0 {
                Err(Broken::Claim(
                    Claim::TeardownReserved,
                    format!(
                        "TeardownReserved: the stopped reactor is gone with {} headers still \
                         reserved",
                        read.reserved
                    ),
                ))
            } else if read.owned != 0 {
                Err(Broken::Claim(
                    Claim::TeardownOwned,
                    format!(
                        "TeardownOwned: the stopped reactor is gone with {} headers still owned",
                        read.owned
                    ),
                ))
            } else if read.locator_permits != free {
                Err(Broken::Claim(
                    Claim::TeardownLocatorPermits,
                    format!(
                        "TeardownLocatorPermits: the stopped reactor is gone with {} of {} \
                         locator permits free, and storage holds {storage_permits}",
                        read.locator_permits,
                        probe.locator_capacity()
                    ),
                ))
            } else {
                Ok(())
            }
        }
    }
}
