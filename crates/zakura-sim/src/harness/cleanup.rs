//! The cleanup comparison (C17): what the scheduler's shutdown does after a
//! run's log froze changes nothing the run ended with.
//!
//! A node's [`EvidenceStores`] freeze with the log and take a snapshot then
//! ([`crate::stores`]). [`run_compared`] runs a harness through
//! [`Sim::run`](crate::Sim::run), which shuts the scheduler down before it
//! returns, and then [`compare`]s a second snapshot of every store with the
//! freeze's, and the log at the freeze with the log the run returned. A
//! direct append to the log would leave every store unchanged, so the log
//! is compared too.

use super::{run_reading, HarnessError, Subsystem};
use crate::{stores::EvidenceStores, NullSink, ObservationLog, PlanInput, RunReport};

/// What changed between a run's freeze and the end of its cleanup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Drift {
    /// The first store whose snapshot changed, by name, or why the stores
    /// can't be compared ([`EvidenceStores::drift`]).
    Store(String),
    /// The run's log changed: how many entries it held at the freeze and
    /// how many the run returned.
    Log {
        /// Entries at the freeze.
        frozen: usize,
        /// Entries in the returned log.
        returned: usize,
    },
}

/// A run and what its cleanup changed.
#[derive(Debug)]
pub struct Compared {
    /// The run's report.
    pub report: RunReport,
    /// What the cleanup changed, or `None` if nothing did.
    pub drift: Option<Drift>,
}

/// What changed between the freeze of `stores` and now, and between
/// `frozen`, the log at the freeze, and `returned`, the log the run
/// returned: the stores first, then the log. `None` if neither changed.
pub fn compare(
    stores: &EvidenceStores,
    frozen: &ObservationLog,
    returned: &ObservationLog,
) -> Option<Drift> {
    if let Some(store) = stores.drift() {
        return Some(Drift::Store(store));
    }
    (frozen != returned).then(|| Drift::Log {
        frozen: frozen.entries().len(),
        returned: returned.entries().len(),
    })
}

/// Runs harness `S` on `scenario`, `prepared` and `input`, as
/// [`run_prepared`](super::run_prepared) does with no sink, and compares
/// `stores`, the node's that `prepared` holds, and the log, after the run
/// returned, with how they stood at the freeze ([`compare`]).
///
/// # Errors
///
/// As [`run_prepared`](super::run_prepared) does: a run that ends at a
/// harness failure has nothing to compare.
pub fn run_compared<S: Subsystem>(
    scenario: &S::Scenario,
    prepared: S::Prepared,
    stores: &EvidenceStores,
    input: PlanInput,
) -> Result<Compared, HarnessError> {
    let (report, frozen) = run_reading::<S, ObservationLog>(
        scenario,
        prepared,
        input,
        &mut NullSink,
        None,
        |_, _, _, log| log.clone(),
    )?;
    let drift = compare(stores, &frozen, &report.log);
    Ok(Compared { report, drift })
}
