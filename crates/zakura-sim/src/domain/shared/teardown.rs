//! Teardown returns to baseline, defined once (properties.md, Properties
//! shared across reactors: Teardown returns to baseline; reg-10.3).
//!
//! A teardown property claims that once a reactor's work ends and its
//! teardown settles, no charge it held remains. Each instance implements
//! [`Teardown`]: its ID and claim, its trigger, its settlement and its
//! charges. [`teardown`] builds the property. Its check is the part every
//! instance shares:
//!
//! - A step before the trigger is `NotApplicable`.
//! - Missing evidence calls `missing_evidence` and is `NotApplicable`.
//! - A step before settlement calls `unsettled(TeardownOpen)` and is
//!   `NotApplicable`. The instance establishes settlement from lifecycle
//!   facts, never from the charges reaching zero.
//! - At settlement, the step holds while every charge is zero. The first
//!   charge that isn't zero is the violation.

use crate::{
    property::{
        CheckCtx, HasView, Inconclusive, Kind, Property, PropertyId, Source, Status,
        TriggerHistory, Verdict,
    },
    ObservationLog,
};

/// One reactor's instance of teardown returning to baseline.
pub trait Teardown<S: HasView> {
    /// The property's ID.
    const ID: &'static str;
    /// The claim, in one sentence.
    const CLAIM: &'static str;
    /// The observation sources the reading reads.
    const NEEDS: &'static [Source];
    /// The committed regression's trigger history.
    const WITNESS: fn(&ObservationLog) -> bool;

    /// What the instance gives after a step.
    fn read(cx: &CheckCtx<'_, S>) -> Reading;
}

/// What a teardown instance gives after a step.
pub enum Reading {
    /// The trigger hasn't fired.
    Untriggered,
    /// The teardown's evidence has a gap, so its settlement and its charges
    /// are unknown.
    MissingEvidence(String),
    /// The trigger fired, and teardown hasn't settled.
    Unsettled,
    /// Teardown settled. Each charge's owner and its value, in the order
    /// the violation text looks for the first one that isn't zero.
    Settled(Vec<(&'static str, u64)>),
}

/// The active property for the teardown `T`.
pub fn teardown<S: HasView, T: Teardown<S>>() -> Property<S> {
    Property {
        id: PropertyId::new(T::ID),
        claim: T::CLAIM,
        kind: Kind::Safety,
        assumes: &[],
        needs: T::NEEDS,
        status: Status::Active,
        check: check::<S, T>,
        witness: Some(TriggerHistory::Log(T::WITNESS)),
    }
}

/// Checks the teardown `T` after a step.
fn check<S: HasView, T: Teardown<S>>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    match T::read(cx) {
        Reading::Untriggered => Verdict::NotApplicable,
        Reading::MissingEvidence(gap) => {
            cx.missing_evidence(gap);
            Verdict::NotApplicable
        }
        Reading::Unsettled => {
            cx.unsettled(Inconclusive::TeardownOpen);
            Verdict::NotApplicable
        }
        Reading::Settled(charges) => match charges.iter().find(|(_, value)| *value > 0) {
            None => Verdict::Holds,
            Some((owner, value)) => cx.violated(
                format!("{owner} is zero once teardown settles"),
                format!("it counts {value}"),
            ),
        },
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
    struct Scripted(RefCell<Option<Reading>>);

    impl HasView for Scripted {
        type View = Option<Reading>;

        fn view(&self) -> Self::View {
            self.0.borrow_mut().take()
        }
    }

    /// A teardown that gives the scripted reading.
    struct ScriptedTeardown;

    fn no_witness(_: &ObservationLog) -> bool {
        false
    }

    impl Teardown<Scripted> for ScriptedTeardown {
        const ID: &'static str = "test/scripted-teardown";
        const CLAIM: &'static str = "no scripted charge remains once teardown settles";
        const NEEDS: &'static [Source] = &[Source::View];
        const WITNESS: fn(&ObservationLog) -> bool = no_witness;

        fn read(cx: &CheckCtx<'_, Scripted>) -> Reading {
            cx.view().expect("each check reads one scripted reading")
        }
    }

    /// What the shared check left after one reading.
    struct Judged {
        verdict: Verdict,
        missing: Option<String>,
        unsettled: Option<Inconclusive>,
    }

    /// The shared check's verdict on `reading`, and what it left behind.
    fn judge(reading: Reading) -> Judged {
        let harness = Scripted(RefCell::new(Some(reading)));
        let registered = [teardown::<Scripted, ScriptedTeardown>()];
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
        let mut cx = CheckCtx::new(&step, &harness, (&registered[0], &registered), None);
        let verdict = (registered[0].check)(&mut cx);
        let parts = cx.into_parts();
        Judged {
            verdict,
            missing: parts.missing,
            unsettled: parts.unsettled,
        }
    }

    #[test]
    fn a_step_before_the_trigger_is_not_applicable_and_records_nothing() {
        let judged = judge(Reading::Untriggered);
        assert_eq!(judged.verdict, Verdict::NotApplicable);
        assert_eq!(judged.missing, None);
        assert_eq!(judged.unsettled, None);
    }

    #[test]
    fn missing_evidence_is_reported_and_not_applicable() {
        let judged = judge(Reading::MissingEvidence("holder gap".to_owned()));
        assert_eq!(judged.verdict, Verdict::NotApplicable);
        assert_eq!(judged.missing.as_deref(), Some("holder gap"));
        assert_eq!(judged.unsettled, None);
    }

    #[test]
    fn a_step_before_settlement_is_teardown_open_and_not_applicable() {
        let judged = judge(Reading::Unsettled);
        assert_eq!(judged.verdict, Verdict::NotApplicable);
        assert_eq!(judged.unsettled, Some(Inconclusive::TeardownOpen));
        assert_eq!(judged.missing, None);
    }

    #[test]
    fn settled_charges_at_zero_hold() {
        let judged = judge(Reading::Settled(vec![("the budget", 0), ("the tally", 0)]));
        assert_eq!(judged.verdict, Verdict::Holds);
        assert_eq!(judged.unsettled, None);
    }

    #[test]
    fn the_first_settled_charge_past_zero_is_the_violation() {
        let judged = judge(Reading::Settled(vec![
            ("the budget", 0),
            ("the tally", 1),
            ("the estimates", 5),
        ]));
        match &judged.verdict {
            Verdict::Violated(violation) => assert_eq!(
                (violation.expected.as_str(), violation.actual.as_str()),
                ("the tally is zero once teardown settles", "it counts 1")
            ),
            other => panic!("expected a violation, got {other:?}"),
        }
        assert_eq!(judged.unsettled, None);
    }
}
