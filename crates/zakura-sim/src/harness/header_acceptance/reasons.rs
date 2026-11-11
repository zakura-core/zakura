//! Every reason a header ID's report can end `NotApplicable` with (C20,
//! item 6).
//!
//! E1 made `NotApplicable` carry a closed [`NaReason`], so the reason
//! inventory is that enum's: [`NA_REASONS`] lists each reason a header ID
//! can end with and what it means, and [`NOT_HEADER`] each one no header
//! ID can. Each listed reason owes a test showing it at its trigger.
//!
//! hdr-14.3's source scan over every `NotApplicable` construction retired
//! with the typed reason. A check's step verdict never names why a run
//! didn't apply, and only the registry builds the run's reason
//! (`registry::outcome`), so the enum is the whole inventory.

use crate::{check::NaReason, property::Pending};

/// One reason a header ID's report can carry, and what it means.
#[derive(Clone, Copy, Debug)]
pub struct NaEntry {
    /// The reason.
    pub reason: NaReason,
    /// What it means for a header ID, in words.
    pub meaning: &'static str,
}

/// An [`NaEntry`] for `reason`, meaning `meaning`.
const fn entry(reason: NaReason, meaning: &'static str) -> NaEntry {
    NaEntry { reason, meaning }
}

/// An `Unsettled` [`NaEntry`] at `site`, meaning `meaning`.
const fn unsettled(site: Pending, meaning: &'static str) -> NaEntry {
    entry(NaReason::Unsettled { site }, meaning)
}

/// Every reason a header ID's report can end `NotApplicable` with, in
/// [`NaReason`]'s order, and each `Unsettled` site a header check marks.
pub static NA_REASONS: &[NaEntry] = &[
    entry(
        NaReason::NeverApplied,
        "the check ran, and its trigger never occurred",
    ),
    entry(
        NaReason::Undeclared,
        "the scenario doesn't declare an assumption the ID needs",
    ),
    entry(
        NaReason::Excluded,
        "the harness's tier excludes a source the ID needs",
    ),
    entry(
        NaReason::AssumptionsNeverHeld,
        "the scenario declares the ID's assumptions, and they never all held",
    ),
    entry(
        NaReason::AssumptionLost,
        "a lost assumption withdrew an open obligation, and nothing applied in the run",
    ),
    entry(
        NaReason::StoppedByViolation,
        "an Active violation stopped the run with the ID's work still open",
    ),
    entry(
        NaReason::HarnessFailed,
        "the run ended at a harness failure",
    ),
    entry(
        NaReason::NodeStopped,
        "a watch read was still open while the harness stopped the node",
    ),
    unsettled(
        Pending::FaultOpen,
        "a penalty or a failed reply was still open at an unsettled end",
    ),
    unsettled(
        Pending::TeardownOpen,
        "a released round or the node's stop hadn't settled at an unsettled end",
    ),
    unsettled(
        Pending::WatchOpen,
        "a published state still waited for its watch read at an unsettled end",
    ),
    unsettled(
        Pending::UnreadTrigger,
        "a departure's trigger came before any read of the fixture it needs",
    ),
    unsettled(
        Pending::UnjudgedDeparture,
        "a departure the slot rule hadn't judged at a unit boundary yet",
    ),
];

/// The reasons no header ID can end with, and why.
pub static NOT_HEADER: &[(NaReason, &str)] = &[
    (
        NaReason::Unsettled {
            site: Pending::UnreadBoundary,
        },
        "the budget boundary mark, which a dry end turns into missing evidence, a harness \
         failure, and which a violation's stop or a harness end outranks",
    ),
    (
        NaReason::Unsettled {
            site: Pending::BlockTeardownOpen,
        },
        "block sync's teardown mark, which only block-sync and sync IDs set",
    ),
];
