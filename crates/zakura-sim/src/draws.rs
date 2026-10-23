//! The draw callback: every runtime draw goes to the run's `Plan` (S4).
//!
//! The loop and the callback share one [`Choices`]. The loop borrows it only
//! to pick, and releases it before any task code runs, so a task can draw
//! while [`Scheduler::run`](zakura_scheduler::Scheduler::run) is active.

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use zakura_scheduler::{Draw, DrawKind, Scheduler, TaskId};

use crate::{
    end::HarnessFailure,
    plan::{default_draw, Decider},
    record::DrawRecord,
    HexU64, PlanInput,
};

/// Who is drawing now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Owner {
    /// Setup code, before the first step.
    Setup,
    /// A step that runs this task.
    Task { task: TaskId, name: String },
    /// A step that runs no task, such as a time advance.
    NoTask,
    /// Between steps, while the loop gathers the enabled set or runs the
    /// checker. A draw here is a harness failure.
    Harness,
    /// The run has ended. Cleanup draws take the rotating default without
    /// plan input, and aren't recorded.
    Done,
}

/// A run's choices: the decider, and the draws of the current step.
#[derive(Debug)]
pub(crate) struct Choices {
    pub(crate) decider: Decider,
    owner: Owner,
    draws: Vec<DrawRecord>,
    /// How many draws each owner has taken of each kind.
    ordinals: BTreeMap<(String, &'static str), u32>,
}

impl Choices {
    /// Choices from `plan`, starting in setup.
    pub(crate) fn new(plan: PlanInput) -> Self {
        Choices {
            decider: Decider::new(plan),
            owner: Owner::Setup,
            draws: Vec::new(),
            ordinals: BTreeMap::new(),
        }
    }

    /// Attributes the next draws to `owner`.
    pub(crate) fn set_owner(&mut self, owner: Owner) {
        self.owner = owner;
    }

    /// The draws since the last call, in order.
    pub(crate) fn take_draws(&mut self) -> Vec<DrawRecord> {
        std::mem::take(&mut self.draws)
    }

    /// Answers `draw` from the plan and records it, or reports why the draw
    /// can't be attributed.
    fn answer(&mut self, draw: &Draw) -> Result<u64, HarnessFailure> {
        match self.owner {
            // Rotating keeps a cleanup `select!` loop from always taking
            // the same branch while preemption points are off.
            Owner::Done => {
                let task = draw
                    .task
                    .map_or_else(|| "setup".to_owned(), |t| t.index().to_string());
                let kind = kind_name(draw.kind).unwrap_or("unknown");
                let n = self
                    .ordinals
                    .entry((format!("cleanup/{task}"), kind))
                    .or_insert(0);
                *n = n.saturating_add(1);
                return Ok(default_draw(draw.bound, *n));
            }
            Owner::Harness => return Err(HarnessFailure::DrawOutsideStep),
            Owner::Setup | Owner::Task { .. } | Owner::NoTask => {}
        }
        let kind = kind_name(draw.kind).ok_or(HarnessFailure::UnknownDrawKind)?;
        let owner = match (&self.owner, draw.task) {
            (Owner::Task { task, name }, Some(drawing)) if *task == drawing => name.clone(),
            (Owner::Setup | Owner::NoTask, None) => "setup".to_owned(),
            _ => {
                return Err(HarnessFailure::DrawFromOtherTask {
                    found: draw.task.map(TaskId::index),
                })
            }
        };
        let n = self.ordinals.entry((owner.clone(), kind)).or_insert(0);
        *n = n.saturating_add(1);
        let ordinal = *n;
        let answer = self
            .decider
            .draw(draw.bound, ordinal)
            .map_err(HarnessFailure::Plan)?;
        self.draws.push(DrawRecord {
            draw: format!("{owner}:{kind}#{ordinal}"),
            bound: HexU64(draw.bound),
            value: HexU64(answer.value),
            by: answer.by,
        });
        Ok(answer.value)
    }
}

/// The first harness failure a draw hit. The loop checks it after setup and
/// after every step.
pub(crate) type Failure = Rc<RefCell<Option<HarnessFailure>>>;

/// Installs the callback that answers `sched`'s draws from `choices`.
///
/// The callback holds `choices` through an `Rc` that the scheduler drops with
/// its run, so it can't outlive the run or reach another one.
pub(crate) fn install(sched: &mut Scheduler, choices: &Rc<RefCell<Choices>>, failure: &Failure) {
    let (choices, failure) = (Rc::clone(choices), Rc::clone(failure));
    sched.set_draws(move |draw| {
        let answer = match choices.try_borrow_mut() {
            Ok(mut choices) => choices.answer(draw),
            Err(_) => Err(HarnessFailure::PlanBorrowedDuringDraw),
        };
        answer.unwrap_or_else(|error| {
            failure.borrow_mut().get_or_insert(error);
            default_draw(draw.bound, 1)
        })
    });
}

/// The name draw identities use for `kind`, or `None` for a kind this crate
/// doesn't know yet.
fn kind_name(kind: DrawKind) -> Option<&'static str> {
    match kind {
        DrawKind::Rng => Some("rng"),
        DrawKind::Select => Some("select"),
        DrawKind::HashSeed => Some("hash-seed"),
        DrawKind::StreamMap => Some("stream-map"),
        _ => None,
    }
}
