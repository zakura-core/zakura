//! Sources whose observations are evidence the run must not lose.
//!
//! An evidence source registers its [`Checkpoint`] with the run when it's
//! created, before anything can wrap it, so the run holds the checkpoint
//! however the source is later added. At the end of each successful
//! `observe`, the source records there what it delivered. The run then
//! requires, at setup and after every step, that the checkpoint names that
//! step and that the run's own log holds exactly what it records. Storage
//! sources and reactor taps are checked the same way, each through its
//! [`LiveEvidence`].

use std::{cell::RefCell, rc::Rc, sync::Weak};

use zakura_storage::header::controlled::ControlledPort;

use crate::{
    observe::{reactor_entries, storage_entries},
    Observation,
};

/// An evidence source's identity: the run that registered it and its index
/// among that run's sources of its kind. Only the index is ever serialized,
/// since run IDs differ between processes.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct EvidenceId {
    /// The registering run, unique within the process.
    pub(crate) run: u64,
    /// The index among that run's sources of the same kind.
    pub(crate) index: u64,
}

/// What publishes a source's evidence, read live at every evidence check,
/// since activity after the source's last observation leaves its mark as it
/// was.
pub(crate) trait LiveEvidence {
    /// The last entry published, 0 before any.
    fn last_seq(&self) -> u64;

    /// Whether something used the publisher outside its run, which it
    /// refused without publishing.
    fn refused_outside_run(&self) -> bool;

    /// Why the publisher's evidence is incomplete, if it is, as a failure
    /// detail.
    fn health(&self) -> Option<&'static str>;
}

/// A controlled port publishes storage facts.
impl LiveEvidence for ControlledPort {
    /// The port's last fact.
    fn last_seq(&self) -> u64 {
        ControlledPort::last_seq(self)
    }

    /// Whether the port refused a call outside the run it's bound to.
    fn refused_outside_run(&self) -> bool {
        ControlledPort::refused_outside_run(self)
    }

    /// Always healthy: a port that's gone has published nothing more.
    fn health(&self) -> Option<&'static str> {
        None
    }
}

/// Which kind of evidence a checkpoint holds, which picks its index space
/// and the log entries it's compared with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EvidenceKind {
    /// A storage source of `service`. Every storage source shares one index
    /// space, whatever its service.
    Storage {
        /// The service, such as `header`.
        service: String,
    },
    /// A reactor tap on the node named `node`, a display label only.
    Reactor {
        /// The node's name.
        node: String,
    },
}

impl EvidenceKind {
    /// Whether `other` shares this kind's index space.
    pub(crate) fn same_space(&self, other: &EvidenceKind) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }

    /// The entries of `observations` that instance `instance` of this kind
    /// delivered, in order.
    pub(crate) fn entries<'o>(
        &self,
        observations: impl IntoIterator<Item = &'o Observation>,
        instance: u64,
    ) -> Vec<&'o Observation> {
        match self {
            EvidenceKind::Storage { service } => storage_entries(observations, service, instance),
            EvidenceKind::Reactor { .. } => reactor_entries(observations, instance),
        }
    }

    /// The source's name in a failure, such as `storage:header#0`.
    pub(crate) fn source(&self, instance: u64) -> String {
        match self {
            EvidenceKind::Storage { service } => format!("storage:{service}#{instance}"),
            EvidenceKind::Reactor { node } => format!("reactor:{node}#{instance}"),
        }
    }

    /// The source, the publisher and what it publishes, in failure details.
    pub(crate) fn words(&self) -> Words {
        match self {
            EvidenceKind::Storage { .. } => Words {
                source: "storage",
                publisher: "port",
                entries: "facts",
                refused: "the port refused use outside its run",
            },
            EvidenceKind::Reactor { .. } => Words {
                source: "tap",
                publisher: "tap",
                entries: "events",
                refused: "the tap refused an emission outside its run",
            },
        }
    }
}

/// An [`EvidenceKind`]'s words in failure details.
pub(crate) struct Words {
    /// The source, such as `storage`.
    pub(crate) source: &'static str,
    /// What publishes its entries, such as `port`.
    pub(crate) publisher: &'static str,
    /// What it publishes, such as `facts`.
    pub(crate) entries: &'static str,
    /// The `RefusedOutsideRun` detail.
    pub(crate) refused: &'static str,
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

/// What an evidence source delivered during one step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Delivered {
    /// The step.
    pub(crate) step: u64,
    /// Every entry it logged, exactly and in order.
    pub(crate) observations: Vec<Observation>,
    /// The last entry it has delivered in the whole run, 0 before any.
    pub(crate) through_seq: u64,
}

/// Where an evidence source records its [`Mark`], shared with the run,
/// with a handle the run reads the source's publisher through.
#[derive(Clone, Debug)]
pub(crate) struct Checkpoint(Rc<Shared>);

/// A checkpoint's contents.
struct Shared {
    mark: RefCell<Mark>,
    kind: EvidenceKind,
    /// The publisher, read live, since activity after the last observation
    /// leaves the mark unchanged.
    live: Weak<dyn LiveEvidence>,
}

impl std::fmt::Debug for Shared {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Shared")
            .field("mark", &self.mark)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl Checkpoint {
    /// A checkpoint of `kind` for a source published by `live`, not yet
    /// observed.
    pub(crate) fn new(kind: EvidenceKind, live: Weak<dyn LiveEvidence>) -> Self {
        Checkpoint(Rc::new(Shared {
            mark: RefCell::default(),
            kind,
            live,
        }))
    }

    /// Records that an observation at `step` delivered `observations`, the
    /// entries it logged, in order, and that the run has now delivered
    /// entries through `through_seq`. A second observation in the same step
    /// adds to the first.
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

    /// The checkpoint's kind.
    pub(crate) fn kind(&self) -> &EvidenceKind {
        &self.0.kind
    }

    /// The source's publisher, while it exists.
    pub(crate) fn live(&self) -> Option<std::sync::Arc<dyn LiveEvidence>> {
        self.0.live.upgrade()
    }
}
