//! Projected write-set product types before and after invariant verification.

use std::sync::Arc;

use crate::graph::GraphDelta;
use crate::{
    ChangeSet, EngineLimits, EngineSnapshot, Frontier, TransitionDomain, TransitionEffect,
};

/// A complete projected write set awaiting independent invariant verification.
#[derive(Clone, Debug)]
pub struct PlanCandidate {
    pub(crate) transition_source: crate::transition::engine::EngineTransitionSource,
    pub(crate) snapshot_before_commit: EngineSnapshot,
    pub(crate) change_set: ChangeSet,
    pub(crate) graph_delta: GraphDelta,
    pub(crate) domain: TransitionDomain,
    pub(crate) effect: TransitionEffect,
    pub(crate) trust_pins: Arc<[Frontier]>,
    pub(crate) limits: EngineLimits,
}

impl PlanCandidate {
    /// Return the opaque graph transition paired with this candidate.
    pub(crate) const fn graph_delta(&self) -> &GraphDelta {
        &self.graph_delta
    }

    /// Return the orthogonal transition effects.
    pub(crate) const fn effect(&self) -> TransitionEffect {
        self.effect
    }
}

/// A verified durable write set ready for one post-commit in-memory installation.
#[cfg_attr(test, derive(Clone))]
#[derive(Debug)]
pub struct EngineTransition {
    candidate: PlanCandidate,
}

impl EngineTransition {
    /// Wrap a candidate that has already passed independent invariant verification.
    pub(super) fn from_verified(candidate: PlanCandidate) -> Self {
        Self { candidate }
    }

    pub(crate) const fn transition_source(
        &self,
    ) -> &crate::transition::engine::EngineTransitionSource {
        &self.candidate.transition_source
    }

    /// Borrow the verified inner candidate (tests and fuzzing only).
    #[cfg(test)]
    pub(crate) const fn candidate(&self) -> &PlanCandidate {
        &self.candidate
    }

    /// Return the atomic write set for the state adapter.
    pub const fn change_set(&self) -> &ChangeSet {
        &self.candidate.change_set
    }

    /// Return the snapshot before commit.
    pub const fn snapshot_before_commit(&self) -> &EngineSnapshot {
        &self.candidate.snapshot_before_commit
    }

    /// Return the snapshot after commit.
    pub fn snapshot_after_commit(&self) -> EngineSnapshot {
        self.candidate.change_set.metadata.snapshot()
    }

    /// Return the submitted transition domain.
    pub const fn domain(&self) -> TransitionDomain {
        self.candidate.domain
    }

    /// Return the orthogonal transition effects.
    pub const fn effect(&self) -> TransitionEffect {
        self.candidate.effect
    }

    /// Return true when the evidence was valid but changed no durable fact.
    pub fn is_no_change(&self) -> bool {
        self.candidate.snapshot_before_commit.state_version
            == self.candidate.change_set.metadata.state_version
    }

    /// Return the opaque graph transition that matches the durable write set.
    pub(crate) const fn graph_delta(&self) -> &GraphDelta {
        &self.candidate.graph_delta
    }
}

#[cfg(test)]
impl std::ops::Deref for EngineTransition {
    type Target = PlanCandidate;

    fn deref(&self) -> &Self::Target {
        &self.candidate
    }
}

#[cfg(test)]
impl std::ops::DerefMut for EngineTransition {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.candidate
    }
}
