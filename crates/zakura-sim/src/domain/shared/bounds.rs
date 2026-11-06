//! A resource bound, defined once (properties.md, Properties shared across
//! reactors: Bounds; reg-10.2).
//!
//! A bound claims that a quantity stays within a limit at each of its
//! boundaries. Each instance implements [`Bound`]: its ID and claim, its
//! unit, its boundary, its quantity reader, its limit and the limit's
//! terms, its peak names and its violation text. [`bound`] builds the
//! property. Its check is the part every instance shares:
//!
//! - A step that isn't a boundary is `NotApplicable`.
//! - Missing evidence calls `missing_evidence` and is `NotApplicable`.
//! - A reading records every peak. It holds while its quantity is within
//!   its limit, and is violated past it.
//! - An instance that judges events instead of steps gives findings. The
//!   check records every peak, and reports the first finding as the verdict
//!   and the rest as recorded violations.

use crate::{
    property::{CheckCtx, HasView, Kind, Property, PropertyId, Source, Status, Unit, Verdict},
    ObservationLog,
};

use super::{report, Finding};

/// One reactor's instance of a resource bound.
pub trait Bound<S: HasView> {
    /// The property's ID.
    const ID: &'static str;
    /// The claim, in one sentence.
    const CLAIM: &'static str;
    /// What the bound counts.
    const UNIT: Unit;
    /// The observation sources the reading reads.
    const NEEDS: &'static [Source];
    /// The committed regression's trigger history.
    const WITNESS: fn(&ObservationLog) -> bool;
    /// What the violation text names besides the quantity and its limit.
    type Terms;

    /// What the instance gives after a step.
    fn read(cx: &CheckCtx<'_, S>) -> Reading<Self::Terms>;

    /// The expected and actual texts of a reading past its limit. The
    /// expected text names the limit and its terms, and the actual text
    /// gives the quantity.
    fn violation(reading: &Measured<Self::Terms>) -> (String, String);
}

/// What a bound's instance gives after a step.
pub enum Reading<T> {
    /// The step isn't one of the bound's boundaries.
    Unjudged,
    /// The bound's evidence has a gap, so its quantity is unknown.
    MissingEvidence(String),
    /// The quantity at this boundary.
    Measured(Measured<T>),
    /// The findings of an instance that judges events, since its last
    /// check.
    Findings {
        /// Each named quantity's value by this step.
        peaks: Vec<(&'static str, u64)>,
        /// The violations found, in order.
        findings: Vec<Finding>,
        /// Whether the run reached an event the instance judges.
        applicable: bool,
    },
}

/// A bound's quantity at one boundary.
pub struct Measured<T> {
    /// The quantity.
    pub quantity: u64,
    /// The limit the quantity must stay within.
    pub limit: u64,
    /// Each named quantity's value at this boundary, for the report's
    /// peaks.
    pub peaks: Vec<(&'static str, u64)>,
    /// What the violation text names besides the quantity and its limit.
    pub terms: T,
}

/// The active property for the bound `B`.
pub fn bound<S: HasView, B: Bound<S>>() -> Property<S> {
    Property {
        id: PropertyId::new(B::ID),
        claim: B::CLAIM,
        kind: Kind::Bound { unit: B::UNIT },
        assumes: &[],
        needs: B::NEEDS,
        status: Status::Active,
        check: check::<S, B>,
        witness: Some(B::WITNESS),
    }
}

/// Checks the bound `B` after a step.
fn check<S: HasView, B: Bound<S>>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    match B::read(cx) {
        Reading::Unjudged => Verdict::NotApplicable,
        Reading::MissingEvidence(gap) => {
            cx.missing_evidence(gap);
            Verdict::NotApplicable
        }
        Reading::Measured(reading) => {
            for &(name, value) in &reading.peaks {
                cx.peak(name, value);
            }
            if reading.quantity <= reading.limit {
                return Verdict::Holds;
            }
            let (expected, actual) = B::violation(&reading);
            cx.violated(expected, actual)
        }
        Reading::Findings {
            peaks,
            findings,
            applicable,
        } => {
            for (name, value) in peaks {
                cx.peak(name, value);
            }
            report(cx, findings).unwrap_or(if applicable {
                Verdict::Holds
            } else {
                Verdict::NotApplicable
            })
        }
    }
}
