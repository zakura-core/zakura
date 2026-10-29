//! Focused Plans built by steering: a test names the history it needs as a
//! rule over each step's enabled set, and [`steer`] turns a starting plan
//! into a scripted one that follows the rule.
//!
//! A run under a scripted plan repeats the picks and draws it names, then
//! goes on by the default rules. So [`steer`] runs the plan, and at the
//! first step whose pick the rule refuses, stops the run, takes every choice
//! the trace recorded before that step, adds the rule's pick, and runs that
//! script. It stops once a run takes no pick the rule refuses. Each round
//! changes one pick, and everything before it replays exactly.

use zakura_sim::{
    check::Phase, By, Checker, ObservationLog, Plan, PlanChoice, PlanInput, PropertyReport, Record,
    RunEnd, RunReport, StepCtx, Violation,
};

/// The property a steered run stops with when its rule refuses a pick.
const STEERED: &str = "test/steered";

/// What a rule says about one step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Steer {
    /// The plan's pick is fine.
    Fine,
    /// The step must pick this identity instead, which it has enabled.
    Pick(String),
}

/// A rule over a run's steps. It reads the step's enabled set and the log,
/// and must judge only what the log held before the step ([`before`]), so
/// a changed pick is judged as the original one was.
pub type Rule<'r> = dyn Fn(&StepCtx<'_>) -> Result<Steer, String> + 'r;

/// The log's entries from before `cx`'s step.
pub fn before<'l>(cx: &StepCtx<'l>) -> &'l [zakura_sim::observe::Entry] {
    let entries = cx.log.entries();
    &entries[..entries.partition_point(|entry| entry.step < cx.step)]
}

/// Stops a run at the first step its rule refuses, recording the pick the
/// rule wants or why it can't be kept.
struct Steering<'r> {
    rule: &'r Rule<'r>,
    /// The first refused step and the rule's answer.
    refused: Option<(u64, Result<String, String>)>,
    /// A step loop violation found at the refused step, which the run's
    /// end then doesn't name.
    also: Option<Violation>,
}

impl Checker for Steering<'_> {
    /// Ends the run at a step whose pick the rule refuses. Drain steps
    /// ignore the plan, so they're never refused.
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        if cx.phase == Phase::Drain || self.refused.is_some() {
            return None;
        }
        let refused = match (self.rule)(cx) {
            Ok(Steer::Fine) => return None,
            Ok(Steer::Pick(want)) if want == cx.pick.to_string() => return None,
            Ok(Steer::Pick(want)) => {
                if !cx.enabled.iter().any(|e| e.identity.to_string() == want) {
                    Err(format!(
                        "step {}: the rule wants {want}, which isn't enabled",
                        cx.step
                    ))
                } else {
                    Ok(want)
                }
            }
            Err(why) => Err(format!("step {}: {why}", cx.step)),
        };
        self.refused = Some((cx.step, refused));
        Some(Violation {
            property: STEERED.to_owned(),
            step: cx.step,
            witness: vec![cx.step],
            expected: "a pick the rule allows".to_owned(),
            actual: cx.pick.to_string(),
        })
    }

    /// Nothing.
    fn at_end(&mut self, _: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        Vec::new()
    }

    /// Keeps the step loop's violation, so a real failure at the refused
    /// step isn't steered away.
    fn step_loop_violation(&mut self, violation: &Violation) {
        self.also.get_or_insert_with(|| violation.clone());
    }
}

/// Every real choice `lines` recorded before step `upto`, as a script:
/// the `init` draws, then each step's pick and draws. Forced picks and
/// bound-1 draws take no input, so they have no entry.
fn choices_before(lines: &[String], upto: u64) -> Vec<PlanChoice> {
    let mut choices = Vec::new();
    let draws = |draws: &[zakura_sim::record::DrawRecord], out: &mut Vec<PlanChoice>| {
        for draw in draws {
            if draw.by != By::Forced {
                out.push(PlanChoice::Draw(draw.value));
            }
        }
    };
    for line in lines {
        match serde_json::from_str::<Record>(line).expect("a run writes valid records") {
            Record::Init(init) => draws(&init.draws, &mut choices),
            Record::Step(step) if step.step < upto => {
                if step.by != By::Forced {
                    choices.push(PlanChoice::Pick(step.pick.clone()));
                }
                draws(&step.draws, &mut choices);
            }
            _ => {}
        }
    }
    choices
}

/// Runs a plan under the checker it's given, and returns the report with
/// its records as JSON lines.
pub type Runner<'r> = dyn FnMut(PlanInput, &mut dyn Checker) -> (RunReport, Vec<String>) + 'r;

/// The most rounds [`steer`] takes, each one changing a pick.
const ROUNDS: usize = 256;

/// Runs `plan` with `run`, and steers it by `rule` until a run takes no
/// pick the rule refuses, running each plan with `run`. Returns the
/// steered run's report, its plan, and how many picks were changed.
///
/// # Panics
///
/// If the rule can't be kept at some step, wants a pick that isn't
/// enabled, or needs more than [`ROUNDS`] changes, or if a run failed at
/// the step the rule refused: a harness failure, another checker's
/// violation, or the step loop's. A changed pick could hide that
/// failure, so it's never retried.
pub fn steer(
    plan: PlanInput,
    rule: &Rule<'_>,
    run: &mut Runner<'_>,
) -> (RunReport, PlanInput, usize) {
    let mut plan = plan;
    for round in 0..ROUNDS {
        let mut steering = Steering {
            rule,
            refused: None,
            also: None,
        };
        let (report, lines) = run(plan.clone(), &mut steering);
        let Some((step, wanted)) = steering.refused else {
            return (report, plan, round);
        };
        let other = report
            .end
            .violation
            .as_ref()
            .filter(|violation| violation.property != STEERED);
        if report.end.harness.is_some() || other.is_some() || steering.also.is_some() {
            panic!(
                "the run failed at step {step}, where steering would change its pick: {:?}, \
                 step loop {:?}",
                report.end, steering.also
            );
        }
        let want = wanted.unwrap_or_else(|why| panic!("the rule can't be kept: {why}"));
        let mut choices = choices_before(&lines, step);
        choices.push(PlanChoice::Pick(want));
        plan = PlanInput::Script(Plan { choices });
    }
    panic!("steering needed more than {ROUNDS} changes");
}

/// Two borrowed checkers run as one, the first before the second, as
/// [`zakura_sim::property::Tee`] runs two owned ones.
pub struct Pair<'a>(pub &'a mut dyn Checker, pub &'a mut dyn Checker);

impl Checker for Pair<'_> {
    /// Runs the first, passes its events to the second, then runs the
    /// second.
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        let first = self.0.after_step(cx);
        self.1.observe_events(&self.0.events());
        let second = self.1.after_step(cx);
        first.or(second)
    }

    /// Ends both, the first's events passed on.
    fn at_end(&mut self, end: &RunEnd, log: &ObservationLog) -> Vec<PropertyReport> {
        let mut reports = self.0.at_end(end, log);
        self.1.observe_events(&self.0.events());
        reports.extend(self.1.at_end(end, log));
        reports
    }

    /// The second's events.
    fn events(&mut self) -> Vec<zakura_sim::property::CheckEvent> {
        self.1.events()
    }

    /// Passes `events` to the first.
    fn observe_events(&mut self, events: &[zakura_sim::property::CheckEvent]) {
        self.0.observe_events(events);
    }

    /// The first's failure, or else the second's.
    fn harness_failure(&mut self) -> Option<zakura_sim::HarnessFailure> {
        self.0
            .harness_failure()
            .or_else(|| self.1.harness_failure())
    }

    /// True only if both bound it. Asks both.
    fn bounds_livelock(&mut self) -> bool {
        let first = self.0.bounds_livelock();
        self.1.bounds_livelock() && first
    }

    /// Passes `violation` to both.
    fn step_loop_violation(&mut self, violation: &Violation) {
        self.0.step_loop_violation(violation);
        self.1.step_loop_violation(violation);
    }
}
