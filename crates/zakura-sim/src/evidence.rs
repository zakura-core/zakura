//! Sources whose observations are evidence the run must not lose.
//!
//! An evidence source sets its [`Checkpoint`] at the end of each successful
//! `observe`. [`Sim::add_evidence`](crate::Sim::add_evidence) keeps the
//! checkpoint itself, so a wrapper around the source can't drop it, and
//! requires it to name the current step at setup and after every step.

use std::{cell::Cell, rc::Rc};

/// The last boundary an evidence source observed.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum Mark {
    /// The setup observation never completed.
    #[default]
    NotObserved,
    /// The source's last completed observation was at this step.
    Observed(u64),
}

/// Where an evidence source records its [`Mark`], shared with the run.
#[derive(Clone, Debug, Default)]
pub struct Checkpoint(Rc<Cell<Mark>>);

impl Checkpoint {
    /// Records that the source completed its observation at `mark`.
    pub fn set(&self, mark: Mark) {
        self.0.set(mark);
    }

    /// The last mark recorded.
    pub fn get(&self) -> Mark {
        self.0.get()
    }
}

/// An evidence source paired with its checkpoint. There's no way back to
/// the bare source, so whatever wraps it keeps the checkpoint.
pub struct Evidence<S> {
    source: S,
    checkpoint: Checkpoint,
}

impl<S> Evidence<S> {
    /// Pairs `source` with the `checkpoint` it sets.
    pub fn new(source: S, checkpoint: Checkpoint) -> Self {
        Evidence { source, checkpoint }
    }

    /// The source wrapped by `wrap`, with the same checkpoint.
    pub fn map<T>(self, wrap: impl FnOnce(S) -> T) -> Evidence<T> {
        Evidence {
            source: wrap(self.source),
            checkpoint: self.checkpoint,
        }
    }

    /// The source and its checkpoint, for the run to register.
    pub(crate) fn into_parts(self) -> (S, Checkpoint) {
        (self.source, self.checkpoint)
    }
}
