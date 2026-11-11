//! A property's outcome as a trace's `end` record writes it.
//!
//! These mirror the serde forms of `zakura-sim`'s `PropertyOutcome`,
//! `NaReason` and `Pending`, which xtask reads but doesn't link. Each is a
//! closed enum, so an outcome, reason or site this build doesn't know is a
//! parse error, never a silent class.

use std::fmt;

use serde::Deserialize;
use serde_json::Value;

/// One property's outcome over a run.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum EndOutcome {
    /// It held, with this many applicable evaluations.
    Held {
        /// How many evaluations applied.
        applicable: u64,
    },
    /// The run gave no answer about it, for `reason`.
    NotApplicable {
        /// Why.
        reason: NaReason,
    },
    /// It was violated.
    Violated {},
}

impl EndOutcome {
    /// Reads an `end` record report's `outcome` value.
    pub(super) fn parse(outcome: &Value) -> Result<Self, String> {
        EndOutcome::deserialize(outcome).map_err(|error| format!("the outcome {outcome}: {error}"))
    }
}

impl fmt::Display for EndOutcome {
    /// Writes `held`, `violated`, or `not-applicable/` and the reason's
    /// name, such as `not-applicable/never-applied`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EndOutcome::Held { .. } => f.write_str("held"),
            EndOutcome::NotApplicable { reason } => write!(f, "not-applicable/{reason}"),
            EndOutcome::Violated {} => f.write_str("violated"),
        }
    }
}

/// Why a property ended `not-applicable`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum NaReason {
    /// Benign: no evaluation applied.
    NeverApplied,
    /// Benign: the scenario doesn't declare the property's assumptions.
    Undeclared,
    /// Benign: the harness's tier excludes a source it needs.
    Excluded,
    /// Incomplete: its declared assumptions never all held.
    AssumptionsNeverHeld,
    /// Incomplete: a lost assumption withdrew an obligation, and nothing
    /// applied.
    AssumptionLost,
    /// Incomplete: an `Active` violation stopped the run with its work open.
    StoppedByViolation,
    /// Incomplete: the run ended at a harness failure.
    HarnessFailed,
    /// Incomplete: a watch read was open while the harness stopped the node.
    NodeStopped,
    /// Incomplete: a pending mark at `site` was left at the run's end.
    Unsettled {
        /// The mark's site.
        site: Pending,
    },
    /// Incomplete: an obligation was still open at the run's end.
    ObligationOpen,
}

impl NaReason {
    /// Whether the reason means there was nothing to judge, rather than
    /// that the run's observations were incomplete.
    pub(super) fn is_benign(self) -> bool {
        matches!(
            self,
            NaReason::NeverApplied | NaReason::Undeclared | NaReason::Excluded
        )
    }
}

impl fmt::Display for NaReason {
    /// Writes the reason's kebab-case name, as `zakura-sim` does, with an
    /// `Unsettled` mark's site after a colon, such as
    /// `unsettled:watch-open`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            NaReason::NeverApplied => "never-applied",
            NaReason::Undeclared => "undeclared",
            NaReason::Excluded => "excluded",
            NaReason::AssumptionsNeverHeld => "assumptions-never-held",
            NaReason::AssumptionLost => "assumption-lost",
            NaReason::StoppedByViolation => "stopped-by-violation",
            NaReason::HarnessFailed => "harness-failed",
            NaReason::NodeStopped => "node-stopped",
            NaReason::Unsettled { site } => return write!(f, "unsettled:{site}"),
            NaReason::ObligationOpen => "obligation-open",
        })
    }
}

/// Where a check left a pending mark.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Pending {
    /// A header fault still open.
    FaultOpen,
    /// A budget boundary read mid-poll.
    UnreadBoundary,
    /// A header teardown still open.
    TeardownOpen,
    /// A block-sync teardown still open.
    BlockTeardownOpen,
    /// A watch read still open.
    WatchOpen,
    /// A departure's trigger with no fixture read.
    UnreadTrigger,
    /// A departure not judged yet.
    UnjudgedDeparture,
}

impl fmt::Display for Pending {
    /// Writes the site's kebab-case name, such as `watch-open`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Pending::FaultOpen => "fault-open",
            Pending::UnreadBoundary => "unread-boundary",
            Pending::TeardownOpen => "teardown-open",
            Pending::BlockTeardownOpen => "block-teardown-open",
            Pending::WatchOpen => "watch-open",
            Pending::UnreadTrigger => "unread-trigger",
            Pending::UnjudgedDeparture => "unjudged-departure",
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// Every reason `zakura-sim` writes parses, in the trace's form, and
    /// reads back as the same name. An unknown reason, site or outcome, or
    /// the old `inconclusive` and bare `not-applicable` forms, fail.
    #[test]
    fn every_reason_parses_in_its_trace_form() {
        let units = [
            "never-applied",
            "undeclared",
            "excluded",
            "assumptions-never-held",
            "assumption-lost",
            "stopped-by-violation",
            "harness-failed",
            "node-stopped",
            "obligation-open",
        ];
        for name in units {
            let parsed = EndOutcome::parse(&json!({"not-applicable": {"reason": name}}));
            assert_eq!(
                parsed.map(|outcome| outcome.to_string()),
                Ok(format!("not-applicable/{name}"))
            );
        }
        for site in [
            "fault-open",
            "unread-boundary",
            "teardown-open",
            "block-teardown-open",
            "watch-open",
            "unread-trigger",
            "unjudged-departure",
        ] {
            let parsed = EndOutcome::parse(
                &json!({"not-applicable": {"reason": {"unsettled": {"site": site}}}}),
            );
            assert_eq!(
                parsed.map(|outcome| outcome.to_string()),
                Ok(format!("not-applicable/unsettled:{site}"))
            );
        }
        assert_eq!(
            EndOutcome::parse(&json!({"held": {"applicable": 3}})),
            Ok(EndOutcome::Held { applicable: 3 })
        );
        assert_eq!(
            EndOutcome::parse(&json!({"violated": {"witness": [3]}})),
            Ok(EndOutcome::Violated {})
        );
        for bad in [
            json!({"not-applicable": {"reason": "lost"}}),
            json!({"not-applicable": {"reason": {"unsettled": {"site": "elsewhere"}}}}),
            json!({"inconclusive": {"reason": "harness-failed"}}),
            json!("not-applicable"),
        ] {
            assert!(EndOutcome::parse(&bad).is_err(), "{bad}");
        }
    }
}
