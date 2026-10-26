//! Sources whose observations are evidence the run must not lose.
//!
//! An evidence source registers its [`Checkpoint`] with the run when it's
//! created, before anything can wrap it, so the run holds the checkpoint
//! however the source is later added. At the end of each successful
//! `observe`, the source records there what it delivered. The run then
//! requires, at setup and after every step, that the checkpoint names that
//! step and that the run's own log holds exactly what it records.

use std::{cell::RefCell, rc::Rc};

/// What an evidence source last observed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum Mark {
    /// The setup observation never completed.
    #[default]
    NotObserved,
    /// What the source delivered at its last observed step.
    Observed(Delivered),
}

/// What a storage source delivered during one step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Delivered {
    /// The step.
    pub(crate) step: u64,
    /// The facts' sequence numbers, in order.
    pub(crate) facts: Vec<u64>,
    /// Each state's watermark, in order.
    pub(crate) states: Vec<u64>,
}

/// Where an evidence source records its [`Mark`], shared with the run.
#[derive(Clone, Debug, Default)]
pub(crate) struct Checkpoint(Rc<RefCell<Mark>>);

impl Checkpoint {
    /// Records that an observation at `step` delivered `facts`, and a state
    /// watermarked at `state` if it logged one. A second observation in the
    /// same step adds to the first.
    pub(crate) fn record(&self, step: u64, facts: &[u64], state: Option<u64>) {
        let mut mark = self.0.borrow_mut();
        match &mut *mark {
            Mark::Observed(delivered) if delivered.step == step => {
                delivered.facts.extend_from_slice(facts);
                delivered.states.extend(state);
            }
            _ => {
                *mark = Mark::Observed(Delivered {
                    step,
                    facts: facts.to_vec(),
                    states: state.into_iter().collect(),
                });
            }
        }
    }

    /// The last mark recorded.
    pub(crate) fn get(&self) -> Mark {
        self.0.borrow().clone()
    }
}
