//! Pure ownership registry and the sole asynchronous completion gate.

use std::{collections::HashMap, num::NonZeroU64};

use crate::{
    BodyWorkAuthority, BodyWorkOwner, EngineSnapshot, HeaderSyncWorkOwner, HeaderWorkAuthority,
    HeaderWorkOwner, SourceId,
};

/// Domain-specific owner facts required by the shared completion registry.
pub trait CompletionOwner: Copy + Eq {
    /// Return the header authority common to header and body work.
    fn header_authority(self) -> HeaderWorkAuthority;
    /// Return verified authority only for body-affecting work.
    fn body_authority(self) -> Option<BodyWorkAuthority>;
    /// Return the exact transport session.
    fn session_id(self) -> u64;
    /// Return the exact request identity.
    fn request_id(self) -> NonZeroU64;
}

impl CompletionOwner for HeaderWorkOwner {
    fn header_authority(self) -> HeaderWorkAuthority {
        self.authority
    }

    fn body_authority(self) -> Option<BodyWorkAuthority> {
        None
    }

    fn session_id(self) -> u64 {
        self.session_id
    }

    fn request_id(self) -> NonZeroU64 {
        self.request_id
    }
}

impl CompletionOwner for BodyWorkOwner {
    fn header_authority(self) -> HeaderWorkAuthority {
        self.authority.header
    }

    fn body_authority(self) -> Option<BodyWorkAuthority> {
        Some(self.authority)
    }

    fn session_id(self) -> u64 {
        self.session_id
    }

    fn request_id(self) -> NonZeroU64 {
        self.request_id
    }
}

impl CompletionOwner for HeaderSyncWorkOwner {
    fn header_authority(self) -> HeaderWorkAuthority {
        self.header_authority()
    }

    fn body_authority(self) -> Option<BodyWorkAuthority> {
        self.body_authority()
    }

    fn session_id(self) -> u64 {
        self.session_id()
    }

    fn request_id(self) -> NonZeroU64 {
        self.request_id()
    }
}

/// Exact reason an asynchronous completion has no remaining authority.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum StaleReason {
    /// Selected-header work generation changed.
    HeaderGeneration,
    /// Verified-body generation changed for work that depends on it.
    VerifiedGeneration,
    /// Finality changed the immutable branch anchor.
    BranchAnchor,
    /// No pending entry exists for this source/request pair.
    MissingOwner,
    /// The pending entry belongs to another branch, session, generation, or target.
    OwnerMismatch,
}

/// Result of the centralized ownership check.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CompletionDecision {
    /// The completion still has exact current authority.
    Current,
    /// The completion is terminally stale and must have no effects.
    Stale(StaleReason),
}

/// Exact pending asynchronous owners, keyed by supplier and request identity.
#[derive(Clone, Debug)]
pub struct PendingOwners<O = HeaderSyncWorkOwner>(HashMap<(SourceId, NonZeroU64), O>);

impl<O> Default for PendingOwners<O> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl<O> PendingOwners<O>
where
    O: CompletionOwner,
{
    /// Register one newly published request, returning any contradictory prior owner.
    pub fn insert(&mut self, source: SourceId, owner: O) -> Option<O> {
        self.0.insert((source, owner.request_id()), owner)
    }

    /// Retire one exact source/request owner.
    pub fn remove(&mut self, source: SourceId, request_id: NonZeroU64) -> Option<O> {
        self.0.remove(&(source, request_id))
    }

    /// Retire every request owned by one source, returning exact retired owners.
    pub fn remove_source(&mut self, source: SourceId) -> Vec<O> {
        let keys: Vec<_> = self
            .0
            .keys()
            .filter(|(candidate, _)| *candidate == source)
            .copied()
            .collect();
        keys.into_iter()
            .filter_map(|key| self.0.remove(&key))
            .collect()
    }

    fn get(&self, source: SourceId, request_id: NonZeroU64) -> Option<O> {
        self.0.get(&(source, request_id)).copied()
    }

    /// Number of exact pending owners.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether no asynchronous owner remains pending.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl PendingOwners<HeaderSyncWorkOwner> {
    /// Retire header-sync owners invalidated by a committed transition before scheduling.
    pub fn apply_retirement(
        &mut self,
        retired: &crate::RetiredWork,
        current: &EngineSnapshot,
    ) -> Vec<HeaderSyncWorkOwner> {
        let keys: Vec<_> = self
            .0
            .iter()
            .filter(|(_, owner)| {
                let header = owner.header_authority();
                (retired.header_generation_changed
                    && header.header_generation != current.header_generation)
                    || (retired.verified_generation_changed
                        && owner.body_authority().is_some_and(|authority| {
                            authority.verified_generation != current.verified_generation
                        }))
                    || retired.owners.contains(owner)
                    || header.branch.anchor_hash != current.frontiers.finalized.hash
            })
            .map(|(key, _)| *key)
            .collect();
        keys.into_iter()
            .filter_map(|key| self.0.remove(&key))
            .collect()
    }
}

/// Sole pure decision point used before any completion effect.
#[derive(Copy, Clone, Debug, Default)]
pub struct CompletionGate;

impl CompletionGate {
    /// Compare one structurally registered attempt with its completion.
    pub fn check_registered<O: CompletionOwner>(
        current: &EngineSnapshot,
        registered: Option<(SourceId, O)>,
        source: SourceId,
        owner: &O,
    ) -> CompletionDecision {
        let header = owner.header_authority();
        if header.header_generation != current.header_generation {
            return CompletionDecision::Stale(StaleReason::HeaderGeneration);
        }
        if owner
            .body_authority()
            .is_some_and(|authority| authority.verified_generation != current.verified_generation)
        {
            return CompletionDecision::Stale(StaleReason::VerifiedGeneration);
        }
        if header.branch.anchor_hash != current.frontiers.finalized.hash {
            return CompletionDecision::Stale(StaleReason::BranchAnchor);
        }
        match registered {
            None => CompletionDecision::Stale(StaleReason::MissingOwner),
            Some((registered_source, registered_owner))
                if registered_source != source || registered_owner != *owner =>
            {
                CompletionDecision::Stale(StaleReason::OwnerMismatch)
            }
            Some(_) => CompletionDecision::Current,
        }
    }

    /// Compare every durable generation, branch anchor, source, session, request, and target fact.
    pub fn check<O: CompletionOwner>(
        current: &EngineSnapshot,
        pending: &PendingOwners<O>,
        source: SourceId,
        owner: &O,
    ) -> CompletionDecision {
        Self::check_registered(
            current,
            pending
                .get(source, owner.request_id())
                .map(|pending_owner| (source, pending_owner)),
            source,
            owner,
        )
    }
}
