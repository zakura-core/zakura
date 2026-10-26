//! Sources whose observations are evidence the run must not lose.
//!
//! An evidence source registers its [`Checkpoint`] with the run when it's
//! created, before anything can wrap it, so the run holds the checkpoint
//! however the source is later added. The source sets it at the end of each
//! successful `observe`, and the run requires it to name the current step
//! at setup and after every step.

use std::{cell::Cell, rc::Rc};

/// The last boundary an evidence source observed.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum Mark {
    /// The setup observation never completed.
    #[default]
    NotObserved,
    /// The source's last completed observation was at this step.
    Observed(u64),
}

/// Where an evidence source records its [`Mark`], shared with the run.
#[derive(Clone, Debug, Default)]
pub(crate) struct Checkpoint(Rc<Cell<Mark>>);

impl Checkpoint {
    /// Records that the source completed its observation at `mark`.
    pub(crate) fn set(&self, mark: Mark) {
        self.0.set(mark);
    }

    /// The last mark recorded.
    pub(crate) fn get(&self) -> Mark {
        self.0.get()
    }
}
