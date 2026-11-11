//! A resource bound, defined once (reg-10 inventory, Bounds; reg-10.2).
//!
//! A bound claims that a quantity stays within a limit at each of its
//! boundaries. Each instance implements [`Bound`]: its ID and claim, its
//! unit, its boundary, its quantity reader, its limit and the limit's
//! terms, its peaks and its violation text. [`bound`] builds the
//! property. Its check is the part every instance shares, and each
//! [`Reading`] variant says how the check judges it.

use crate::{
    property::{
        CheckCtx, HasView, Kind, Property, PropertyId, Source, Status, TriggerHistory, Unit,
        Verdict,
    },
    ObservationLog,
};

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
    /// The values the check records as peaks, by name. An array, such as
    /// `[(&'static str, u64); 2]`, lets a step record them without
    /// allocating.
    type Peaks: AsRef<[(&'static str, u64)]>;

    /// What the instance gives after a step.
    fn read(cx: &CheckCtx<'_, S>) -> Reading<Self::Terms, Self::Peaks>;

    /// The expected and actual texts of a quantity past its limit. The
    /// expected text names the limit and its terms, and the actual text
    /// gives the quantity.
    fn violation(measured: &Measured<Self::Terms, Self::Peaks>) -> (String, String);
}

/// What a bound's instance gives after a step.
pub enum Reading<T, P> {
    /// The step isn't one of the bound's boundaries. The check is
    /// `NotApplicable`.
    Unjudged,
    /// The bound's evidence has a gap, so its quantity is unknown. The
    /// check calls `missing_evidence` and is `NotApplicable`.
    MissingEvidence(String),
    /// The quantity at this boundary. The check records every peak. It
    /// holds while the quantity is within its limit, and is violated past
    /// it.
    Measured(Measured<T, P>),
}

/// A bound's quantity at one boundary.
pub struct Measured<T, P> {
    /// The quantity.
    pub quantity: u64,
    /// The limit the quantity must stay within.
    pub limit: u64,
    /// The instance's [`Bound::Peaks`].
    pub peaks: P,
    /// The instance's [`Bound::Terms`].
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
        witness: Some(TriggerHistory::Log(B::WITNESS)),
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
        Reading::Measured(measured) => {
            for &(name, value) in measured.peaks.as_ref() {
                cx.peak(name, value);
            }
            if measured.quantity <= measured.limit {
                return Verdict::Holds;
            }
            let (expected, actual) = B::violation(&measured);
            cx.violated(expected, actual)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use zakura_scheduler::{SimInstant, Status as SchedulerStatus};

    use super::*;
    use crate::{
        check::{Phase, StepCtx},
        Identity,
    };

    /// A harness whose view is the next reading, which the test sets.
    #[derive(Default)]
    struct Scripted(RefCell<Option<ScriptedReading>>);

    /// The scripted bound's reading: terms text and one peak.
    type ScriptedReading = Reading<&'static str, [(&'static str, u64); 1]>;

    impl HasView for Scripted {
        type View = Option<ScriptedReading>;

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
        type Peaks = [(&'static str, u64); 1];

        fn read(cx: &CheckCtx<'_, Scripted>) -> ScriptedReading {
            cx.view().expect("each check reads one scripted reading")
        }

        fn violation(measured: &Measured<&'static str, Self::Peaks>) -> (String, String) {
            (
                format!("at most {} ({})", measured.limit, measured.terms),
                format!("{}", measured.quantity),
            )
        }
    }

    /// What the shared check left after one reading.
    struct Judged {
        verdict: Verdict,
        peaks: Vec<(&'static str, u64)>,
        missing: Option<String>,
    }

    /// The shared check's verdict on `reading`, and what it left behind.
    fn judge(reading: ScriptedReading) -> Judged {
        let harness = Scripted(RefCell::new(Some(reading)));
        let registered = [bound::<Scripted, ScriptedBound>()];
        let log = ObservationLog::default();
        let pick = Identity::advance_time(SimInstant::ZERO);
        let step = StepCtx {
            step: 7,
            pick: &pick,
            enabled: &[],
            phase: Phase::Live,
            budget: u64::MAX,
            now: SimInstant::ZERO,
            status: SchedulerStatus::Quiescent,
            log: &log,
        };
        let mut cx = CheckCtx::new(&step, &harness, (&registered[0], &registered), None);
        let verdict = (registered[0].check)(&mut cx);
        let parts = cx.into_parts();
        Judged {
            verdict,
            peaks: parts.peaks,
            missing: parts.missing,
        }
    }

    fn measured(quantity: u64, limit: u64) -> ScriptedReading {
        Reading::Measured(Measured {
            quantity,
            limit,
            peaks: [("quantity", quantity)],
            terms: "terms",
        })
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
}
