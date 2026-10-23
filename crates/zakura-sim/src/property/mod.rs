//! The property registry (properties.md, Registry).
//!
//! A [`Property`] is a named claim plus a check. A [`Registry`] holds a
//! harness's properties, and [`RegistryChecker`] evaluates them as a run's
//! [`Checker`](crate::Checker): after every step, it runs each check, tracks
//! each property's assumptions and obligations, and reports each property's
//! outcome at run end.

mod assume;
mod cx;
mod id;
mod registry;
mod verdict;

use serde::{Deserialize, Serialize};

pub use assume::{Assume, AssumeRecord, Assumptions};
pub use cx::{CheckCtx, HasView};
pub use id::{BadId, PropertyId};
pub use registry::{CheckEvent, Lifecycle, Registry, RegistryChecker, RegistryError, Tee};
pub use verdict::{Inconclusive, LivenessBound, Obligation, ObligationKey, Subject, Verdict};

use crate::ObservationLog;

/// A named claim about the code under test, and the check that tests it.
pub struct Property<S: HasView> {
    /// The property's stable ID.
    pub id: PropertyId,
    /// The claim, in one sentence.
    pub claim: &'static str,
    /// What kind of claim it is.
    pub kind: Kind,
    /// What the scenario must guarantee. Empty for safety properties.
    pub assumes: &'static [Assume],
    /// The observation sources the check reads.
    pub needs: &'static [Source],
    /// Whether a violation blocks CI.
    pub status: Status,
    /// Evaluates the property after a step.
    pub check: fn(&mut CheckCtx<'_, S>) -> Verdict,
    /// Checks a committed regression's trigger history: the identities and
    /// order of the steps the bug needs. `None` for a property with no
    /// committed regression.
    pub witness: Option<fn(&ObservationLog) -> bool>,
}

/// What kind of claim a property makes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// Must hold after every step.
    Safety,
    /// A numeric resource bound.
    Bound {
        /// What the bound counts.
        unit: Unit,
    },
    /// Every obligation the check opens must be discharged within `bound`.
    Liveness {
        /// The obligations' bound.
        bound: LivenessBound,
    },
}

/// What a resource bound counts (properties.md, Units). Mixing units hides
/// bugs, so each bound names one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Unit {
    /// A request's size estimate, reserved before the body exists.
    EstimateBytes,
    /// The serialized size of a received body.
    WireBytes,
    /// Wire bytes plus decoded cost.
    ResidentBytes,
    /// Bytes a counting allocator attributes to a body.
    AttributedHeap,
    /// Headers.
    Headers,
}

/// An observation source a check reads (properties.md, Tiers). A harness
/// names the sources it observes, and a property that needs one it lacks is
/// `NotApplicable`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Source {
    /// Frames peers send and receive.
    Frames,
    /// Stream and connection closes.
    Closes,
    /// Datagrams on the simulated network.
    Datagrams,
    /// Storage requests and completions.
    Storage,
    /// Gauges and counters.
    Gauges,
    /// The harness's view.
    View,
    /// Scheduler state: tasks and preemption points.
    Schedule,
}

/// Whether a property's violation blocks CI (properties.md, Statuses). Its
/// JSON matches the trace `meta` record's property statuses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum Status {
    /// An enforced claim. A violation ends the run.
    Active,
    /// An open bug violates it. A violation is recorded, and the run goes
    /// on.
    Known {
        /// The bug's issue number.
        issue: u32,
    },
    /// The claim itself is under test. A violation is recorded, and the run
    /// goes on.
    Draft,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The JSON matches the trace `meta` record's property statuses.
    #[test]
    fn status_json() {
        for (status, json) in [
            (Status::Active, r#""active""#),
            (Status::Known { issue: 1523 }, r#"{"known":{"issue":1523}}"#),
            (Status::Draft, r#""draft""#),
        ] {
            assert_eq!(serde_json::to_string(&status).ok().as_deref(), Some(json));
            assert_eq!(serde_json::from_str::<Status>(json).ok(), Some(status));
        }
    }
}
