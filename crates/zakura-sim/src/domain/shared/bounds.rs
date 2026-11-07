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

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use zakura_scheduler::{SimInstant, Status as SchedulerStatus};

    use super::*;
    use crate::{
        check::{Phase, StepCtx, Violation},
        Identity,
    };

    /// A harness whose view is the next reading, which the test sets.
    #[derive(Default)]
    struct Scripted(RefCell<Option<Reading<&'static str>>>);

    impl HasView for Scripted {
        type View = Option<Reading<&'static str>>;

        fn view(&self) -> Self::View {
            self.0.borrow_mut().take()
        }
    }

    /// A bound that gives the scripted reading.
    struct ScriptedBound;

    fn no_witness(_: &ObservationLog) -> bool {
        false
    }

    impl Bound<Scripted> for ScriptedBound {
        const ID: &'static str = "test/scripted-bound";
        const CLAIM: &'static str = "the scripted quantity stays within its limit";
        const UNIT: Unit = Unit::WireBytes;
        const NEEDS: &'static [Source] = &[Source::View];
        const WITNESS: fn(&ObservationLog) -> bool = no_witness;
        type Terms = &'static str;

        fn read(cx: &CheckCtx<'_, Scripted>) -> Reading<&'static str> {
            cx.view().expect("each check reads one scripted reading")
        }

        fn violation(reading: &Measured<&'static str>) -> (String, String) {
            (
                format!("at most {} ({})", reading.limit, reading.terms),
                format!("{}", reading.quantity),
            )
        }
    }

    /// What the shared check left after one reading.
    struct Judged {
        verdict: Verdict,
        peaks: Vec<(&'static str, u64)>,
        missing: Option<String>,
        recorded: Vec<Violation>,
    }

    /// The shared check's verdict on `reading`, and what it left behind.
    fn judge(reading: Reading<&'static str>) -> Judged {
        let harness = Scripted(RefCell::new(Some(reading)));
        let registered = [bound::<Scripted, ScriptedBound>()];
        let log = ObservationLog::default();
        let pick = Identity::advance_time(SimInstant::ZERO);
        let step = StepCtx {
            step: 7,
            pick: &pick,
            enabled: &[],
            phase: Phase::Live,
            now: SimInstant::ZERO,
            status: SchedulerStatus::Quiescent,
            log: &log,
        };
        let mut cx = CheckCtx::new(&step, &harness, &registered[0], &registered);
        let verdict = (registered[0].check)(&mut cx);
        let parts = cx.into_parts();
        Judged {
            verdict,
            peaks: parts.peaks,
            missing: parts.missing,
            recorded: parts.recorded,
        }
    }

    fn measured(quantity: u64, limit: u64) -> Reading<&'static str> {
        Reading::Measured(Measured {
            quantity,
            limit,
            peaks: vec![("quantity", quantity)],
            terms: "terms",
        })
    }

    fn finding(expected: &str) -> Finding {
        Finding {
            step: 7,
            expected: expected.to_owned(),
            actual: "actual".to_owned(),
        }
    }

    /// The expected and actual texts of a violated verdict.
    fn texts(verdict: &Verdict) -> (&str, &str) {
        match verdict {
            Verdict::Violated(violation) => (&violation.expected, &violation.actual),
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    #[test]
    fn a_step_off_the_boundary_is_not_applicable_and_records_nothing() {
        let judged = judge(Reading::Unjudged);
        assert_eq!(judged.verdict, Verdict::NotApplicable);
        assert!(judged.peaks.is_empty());
        assert_eq!(judged.missing, None);
    }

    #[test]
    fn missing_evidence_is_reported_and_not_applicable() {
        let judged = judge(Reading::MissingEvidence("holder gap".to_owned()));
        assert_eq!(judged.verdict, Verdict::NotApplicable);
        assert_eq!(judged.missing.as_deref(), Some("holder gap"));
        assert!(judged.peaks.is_empty());
    }

    #[test]
    fn a_quantity_at_its_limit_holds_and_one_past_it_violates() {
        let judged = judge(measured(10, 10));
        assert_eq!(judged.verdict, Verdict::Holds);
        assert_eq!(judged.peaks, [("quantity", 10)]);

        let judged = judge(measured(11, 10));
        assert_eq!(texts(&judged.verdict), ("at most 10 (terms)", "11"));
        assert_eq!(judged.peaks, [("quantity", 11)]);
    }

    #[test]
    fn findings_record_their_peaks_and_report_the_first() {
        let peaks = vec![("peak", 3)];
        for applicable in [false, true] {
            let judged = judge(Reading::Findings {
                peaks: peaks.clone(),
                findings: Vec::new(),
                applicable,
            });
            let expected = if applicable {
                Verdict::Holds
            } else {
                Verdict::NotApplicable
            };
            assert_eq!(judged.verdict, expected, "applicable: {applicable}");
            assert_eq!(judged.peaks, peaks, "applicable: {applicable}");
        }

        let judged = judge(Reading::Findings {
            peaks: peaks.clone(),
            findings: vec![finding("first"), finding("second")],
            applicable: false,
        });
        assert_eq!(texts(&judged.verdict), ("first", "actual"));
        let recorded: Vec<&str> = judged
            .recorded
            .iter()
            .map(|violation| violation.expected.as_str())
            .collect();
        assert_eq!(recorded, ["second"]);
        assert_eq!(judged.peaks, peaks);
    }
}
