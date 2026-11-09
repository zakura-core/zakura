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
//!   the reactor is gone without unwinding, stopped by the harness or
//!   exited by itself, nothing stays reserved or owned,
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
    /// Whether it was a teardown read, after the reactor was gone without
    /// unwinding.
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

/// What a read is judged against from the probe itself: its budget's and
/// its locator semaphore's sizes.
#[derive(Clone, Copy, Debug)]
pub(super) struct Limits {
    /// The headers the budget can hold, reserved and owned together.
    pub(super) capacity: usize,
    /// The locator permits the semaphore holds.
    pub(super) locator_capacity: usize,
}

impl Limits {
    /// `probe`'s sizes.
    pub(super) fn of(probe: &HeaderResourceProbe) -> Limits {
        Limits {
            capacity: probe.capacity(),
            locator_capacity: probe.locator_capacity(),
        }
    }
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

/// Checks `read` from a probe of `limits` against `expected`, as the module
/// doc lists the rules. Returns every claim rule it breaks, in that order,
/// or the first integrity rule it breaks. An over-capacity read isn't
/// compared with the queue's accounting, since that integrity break would
/// fail the run and hide the claim.
pub(super) fn check(
    limits: Limits,
    read: HeaderResources,
    expected: Expected,
) -> Result<Vec<(Claim, String)>, String> {
    let mut broken = Vec::new();
    let claimed = read.reserved.saturating_add(read.owned);
    if claimed > limits.capacity {
        broken.push((
            Claim::BudgetOverCapacity,
            format!(
                "BudgetOverCapacity: the header budget holds {} reserved and {} owned, over \
                 its {} headers",
                read.reserved, read.owned, limits.capacity
            ),
        ));
    }
    match expected {
        Expected::Live { accounted: None } => {}
        Expected::Live { .. } if !broken.is_empty() => {}
        Expected::Live {
            accounted: Some((reserved, owned)),
        } => {
            if read.reserved != reserved {
                return Err(format!(
                    "ReservedMismatch: at a quiet boundary the header budget holds {} \
                     reserved, and the queue's accounting {reserved}",
                    read.reserved
                ));
            }
            if read.owned != owned {
                return Err(format!(
                    "OwnedMismatch: at a quiet boundary the header budget holds {} owned, \
                     and the queue's accounting {owned}",
                    read.owned
                ));
            }
        }
        Expected::Teardown { storage_permits } => {
            let free = limits.locator_capacity.saturating_sub(storage_permits);
            if read.reserved != 0 {
                broken.push((
                    Claim::TeardownReserved,
                    format!(
                        "TeardownReserved: the reactor is gone with {} headers still \
                         reserved",
                        read.reserved
                    ),
                ));
            }
            if read.owned != 0 {
                broken.push((
                    Claim::TeardownOwned,
                    format!(
                        "TeardownOwned: the reactor is gone with {} headers still owned",
                        read.owned
                    ),
                ));
            }
            if read.locator_permits != free {
                broken.push((
                    Claim::TeardownLocatorPermits,
                    format!(
                        "TeardownLocatorPermits: the reactor is gone with {} of {} \
                         locator permits free, and storage holds {storage_permits}",
                        read.locator_permits, limits.locator_capacity
                    ),
                ));
            }
        }
    }
    Ok(broken)
}

#[cfg(test)]
mod tests {
    use zakura_network::zakura::observe::HeaderResources;

    use super::{check, Expected, Limits};
    use crate::harness::header_node::claim::Claim;

    /// A probe of 10 headers and 4 locator permits.
    const LIMITS: Limits = Limits {
        capacity: 10,
        locator_capacity: 4,
    };

    /// A read of `reserved` and `owned` headers with `locator_permits`
    /// free.
    fn read(reserved: usize, owned: usize, locator_permits: usize) -> HeaderResources {
        HeaderResources {
            reserved,
            owned,
            locator_permits,
        }
    }

    /// The claims `check` finds, by rule.
    fn claims(read: HeaderResources, expected: Expected) -> Vec<Claim> {
        check(LIMITS, read, expected)
            .expect("no integrity rule")
            .into_iter()
            .map(|(claim, _)| claim)
            .collect()
    }

    /// C21: a teardown read that breaks several claim rules reports each,
    /// in the module doc's order, not just the first. Each rule alone
    /// breaks only itself, and a clean read breaks none.
    #[test]
    fn a_teardown_read_reports_every_claim_it_breaks() {
        let teardown = Expected::Teardown { storage_permits: 1 };
        assert_eq!(claims(read(0, 0, 3), teardown), []);
        assert_eq!(claims(read(1, 0, 3), teardown), [Claim::TeardownReserved]);
        assert_eq!(claims(read(0, 1, 3), teardown), [Claim::TeardownOwned]);
        assert_eq!(
            claims(read(0, 0, 4), teardown),
            [Claim::TeardownLocatorPermits]
        );
        assert_eq!(
            claims(read(1, 2, 2), teardown),
            [
                Claim::TeardownReserved,
                Claim::TeardownOwned,
                Claim::TeardownLocatorPermits
            ]
        );
        assert_eq!(
            claims(read(6, 5, 3), teardown),
            [
                Claim::BudgetOverCapacity,
                Claim::TeardownReserved,
                Claim::TeardownOwned
            ]
        );
        let details: Vec<String> = check(LIMITS, read(1, 2, 2), teardown)
            .expect("no integrity rule")
            .into_iter()
            .map(|(_, detail)| detail)
            .collect();
        assert_eq!(
            details,
            [
                "TeardownReserved: the stopped reactor is gone with 1 headers still reserved",
                "TeardownOwned: the stopped reactor is gone with 2 headers still owned",
                "TeardownLocatorPermits: the stopped reactor is gone with 2 of 4 locator \
                 permits free, and storage holds 1",
            ]
        );
    }

    /// A live read over capacity reports the claim and skips the
    /// accounting comparison, and a quiet read off the accounting fails
    /// the run with its first mismatch.
    #[test]
    fn a_live_read_judges_capacity_before_accounting() {
        let quiet = Expected::Live {
            accounted: Some((0, 0)),
        };
        assert_eq!(claims(read(6, 5, 4), quiet), [Claim::BudgetOverCapacity]);
        assert_eq!(
            check(LIMITS, read(1, 1, 4), quiet).map(|claims| claims.len()),
            Err(
                "ReservedMismatch: at a quiet boundary the header budget holds 1 reserved, \
                 and the queue's accounting 0"
                    .to_owned()
            )
        );
        assert_eq!(
            claims(read(1, 1, 4), Expected::Live { accounted: None }),
            []
        );
    }
}
