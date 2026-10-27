//! Sources whose observations are evidence the run must not lose.
//!
//! An evidence source registers its [`Checkpoint`] with the run when it's
//! created, before anything can wrap it, so the run holds the checkpoint
//! however the source is later added. At the end of each successful
//! `observe`, the source records there what it delivered. The run then
//! requires, at setup and after every step, that the checkpoint names that
//! step and that the run's own log holds exactly what it records.

use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Weak},
};

use zakura_storage::header::controlled::ControlledPort;

use crate::Observation;

/// A storage source's identity: the run that registered it and its index
/// among that run's storage sources. Only the index is ever serialized,
/// since run IDs differ between processes.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct StorageId {
    /// The registering run, unique within the process.
    pub(crate) run: u64,
    /// The index among that run's storage sources.
    pub(crate) index: u64,
}

/// What an evidence source last observed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum Mark {
    /// The setup observation never completed.
    #[default]
    NotObserved,
    /// What the source delivered at its last observed step.
    Observed(Delivered),
    /// The source was dropped. The run checks marks only while it's live,
    /// so this fails a source dropped then, and not one dropped at the
    /// run's own shutdown.
    Dropped,
}

/// What a storage source delivered during one step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Delivered {
    /// The step.
    pub(crate) step: u64,
    /// Every fact and state it logged, exactly and in order.
    pub(crate) observations: Vec<Observation>,
    /// The last fact it has delivered in the whole run, 0 before any.
    pub(crate) through_seq: u64,
}

/// Where an evidence source records its [`Mark`], shared with the run,
/// with a handle the run reads the source's port through.
#[derive(Clone, Debug)]
pub(crate) struct Checkpoint(Rc<Shared>);

/// A checkpoint's contents.
#[derive(Debug)]
struct Shared {
    mark: RefCell<Mark>,
    /// The port, read live, since activity after the last observation
    /// leaves the mark unchanged.
    port: Weak<ControlledPort>,
}

impl Checkpoint {
    /// A checkpoint for a source of `port`, not yet observed.
    pub(crate) fn new(port: &Arc<ControlledPort>) -> Self {
        Checkpoint(Rc::new(Shared {
            mark: RefCell::default(),
            port: Arc::downgrade(port),
        }))
    }

    /// Records that an observation at `step` delivered `observations`, the
    /// facts and state it logged, in order, and that the run has now
    /// delivered facts through `through_seq`. A second observation in the
    /// same step adds to the first.
    pub(crate) fn record(&self, step: u64, observations: &[Observation], through_seq: u64) {
        let mut mark = self.0.mark.borrow_mut();
        match &mut *mark {
            Mark::Observed(delivered) if delivered.step == step => {
                delivered.observations.extend_from_slice(observations);
                delivered.through_seq = through_seq;
            }
            _ => {
                *mark = Mark::Observed(Delivered {
                    step,
                    observations: observations.to_vec(),
                    through_seq,
                });
            }
        }
    }

    /// Records that the source was dropped.
    pub(crate) fn dropped(&self) {
        *self.0.mark.borrow_mut() = Mark::Dropped;
    }

    /// The last mark recorded.
    pub(crate) fn get(&self) -> Mark {
        self.0.mark.borrow().clone()
    }

    /// The source's port, while it exists.
    pub(crate) fn port(&self) -> Option<Arc<ControlledPort>> {
        self.0.port.upgrade()
    }
}
