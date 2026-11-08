//! `header-sync/commit-only-authorized`'s owner binding and path checks, on
//! the reactor's facts as its tap receives them.
//!
//! The model is the fixture: every header a run can hold is
//! [`HeaderFixture::extend`]'s child of its parent on branch 0, 1 or 2,
//! so a page's headers are checked one by one as children of the header
//! before, with no list of chains. Authority comes from what the node
//! asked for, the `Open` the subscriber built for each subscription, and
//! never from production's own guards.
//!
//! - **Owner binding.** At each ordinary `Started`, an `Open` was built for
//!   the round's subscription, the round's peer, session and request ID.
//!   A round on a new `Open` hands on exactly the `Open`'s locators. The
//!   round's owner names that subscription's session and request ID, its
//!   branch target is the `Open`'s target, and its anchor is the reactor's
//!   adopted one. Whether its generation is current is another property's.
//! - **Path.** At each `PageStaged`, the page's first header is a fixture
//!   child of its declared ancestor, and each later one of the header
//!   before. A subscription's first page declares one of its `Open`'s
//!   locators, and each later page the last header the subscription
//!   staged. No header is above the target's height, a header at that
//!   height is the target and ends its page, and no header but the target
//!   is one of the `Open`'s locators: only a page's ancestor may be. A
//!   prefix that hasn't reached the target may lie on another fork, since
//!   the node can't know until it gets there.
//!
//! Provenance, a page staged on another session's or subscription's round,
//! is the ledger's `ForeignPage`, and the session view's identity joins are
//! the session evidence's, both claims of the same property.
//!
//! The check applies once it judged a `Started` or a `PageStaged`. A broken
//! rule is a claim break, which the tap hands
//! [`Claim::CommitOnlyAuthorized`](super::claim::Claim) at its drain.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, PoisonError},
};

use zakura_chain::block;
use zakura_header_chain::Frontier;
use zakura_network::zakura::{
    observe::{CreditKind, ReactorEvent},
    HeaderTargetPurpose, ZakuraPeerId,
};
use zakura_storage::header::fixture::HeaderFixture;

/// A subscription, by its peer, session and ID.
type Subscription = (ZakuraPeerId, u64, u64);

/// The fixture branches a header can be on.
const BRANCHES: [u8; 3] = [0, 1, 2];

/// An `Open` as the subscriber built it.
#[derive(Clone, Debug)]
struct Open {
    target: block::Hash,
    target_height: block::Height,
    locators: Vec<block::Hash>,
}

/// What [`AuthorityEvidence`] keeps.
#[derive(Debug, Default)]
struct Store {
    /// The first drain that found the check applicable.
    applicable: Option<u64>,
    /// Whether a fact since the last drain made it applicable.
    pending: bool,
}

/// When the authority check first applied, shared by the tap and whoever
/// reads it. Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct AuthorityEvidence(Arc<Mutex<Store>>);

impl AuthorityEvidence {
    /// Runs `change` on the store. Nothing else is called while the lock
    /// is held.
    fn with<R>(&self, change: impl FnOnce(&mut Store) -> R) -> R {
        change(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Whether a drain up to and including `step`'s found the check
    /// applicable.
    pub fn applicable_by(&self, step: u64) -> bool {
        self.with(|store| store.applicable.is_some_and(|found| found <= step))
    }

    /// Whether any drain found the check applicable.
    pub fn any(&self) -> bool {
        self.with(|store| store.applicable.is_some())
    }
}

/// The check's state.
pub(super) struct Authority {
    fixture: HeaderFixture,
    /// The reactor's adopted finality anchor.
    anchor: Option<block::Hash>,
    /// Each subscription's `Open`.
    opens: HashMap<Subscription, Open>,
    /// The subscriptions a round started on.
    started: HashSet<Subscription>,
    /// Each subscription's last staged header.
    staged: HashMap<Subscription, Frontier>,
    breaks: Vec<String>,
    evidence: AuthorityEvidence,
}

impl Authority {
    /// A check on `fixture`'s headers that keeps when it applied in
    /// `evidence`.
    pub(super) fn new(evidence: AuthorityEvidence, fixture: HeaderFixture) -> Authority {
        Authority {
            fixture,
            anchor: None,
            opens: HashMap::new(),
            started: HashSet::new(),
            staged: HashMap::new(),
            breaks: Vec::new(),
            evidence,
        }
    }

    /// Judges `event`, which the reactor's own task reported when
    /// `on_reactor`, then notes what it says.
    pub(super) fn observe(&mut self, event: &ReactorEvent, on_reactor: bool) {
        if !on_reactor {
            return;
        }
        match event {
            ReactorEvent::SnapshotAdopted { finalized, .. } => self.anchor = Some(finalized.hash),
            ReactorEvent::CreditIssued {
                kind: CreditKind::Open,
                peer,
                session_id,
                subscription,
                update,
                target_height,
                ..
            } => {
                self.opens.insert(
                    (peer.clone(), *session_id, *subscription),
                    Open {
                        target: update.initial_target_tip_hash,
                        target_height: *target_height,
                        locators: update.locator_hashes.clone(),
                    },
                );
            }
            ReactorEvent::Started {
                request,
                purpose: HeaderTargetPurpose::Normal,
                locator,
            } => {
                self.applies();
                let subscription = (
                    request.peer.clone(),
                    request.owner.session_id(),
                    request.request_id,
                );
                self.bind(request.incarnation, &subscription, request, locator);
                self.started.insert(subscription);
            }
            ReactorEvent::PageStaged {
                request,
                subscription,
                ancestor,
                hashes,
                ..
            } => {
                self.applies();
                let subscription = (request.peer.clone(), request.session_id, *subscription);
                self.path(request.incarnation, &subscription, *ancestor, hashes);
            }
            _ => {}
        }
    }

    /// The breaks found since the last call. Marks the check applicable at
    /// `step` if a fact since the last call made it so.
    pub(super) fn drain(&mut self, step: u64) -> Vec<String> {
        self.evidence.with(|store| {
            if std::mem::take(&mut store.pending) {
                store.applicable.get_or_insert(step);
            }
        });
        std::mem::take(&mut self.breaks)
    }

    /// Notes that a fact made the check applicable.
    fn applies(&self) {
        self.evidence.with(|store| store.pending = true);
    }

    /// Judges round `incarnation`'s owner binding to its subscription's
    /// `Open`.
    fn bind(
        &mut self,
        incarnation: u64,
        subscription: &Subscription,
        request: &zakura_network::zakura::observe::RequestRef,
        locator: &[block::Hash],
    ) {
        let Some(open) = self.opens.get(subscription) else {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: round {incarnation} started on session {} subscription \
                 {}, which the subscriber never opened",
                subscription.1, subscription.2
            ));
            return;
        };
        let mut found = Vec::new();
        if !self.started.contains(subscription) && locator != open.locators {
            found.push(format!(
                "handed on locator {locator:?}, not its Open's {:?}",
                open.locators
            ));
        }
        let owner = request.owner;
        let authority = owner.header_authority();
        if owner.request_id().get() != request.request_id {
            found.push(format!(
                "has an owner for request {}, not its subscription {}",
                owner.request_id(),
                request.request_id
            ));
        }
        if authority.branch.target_tip_hash != open.target {
            found.push(format!(
                "has an owner targeting {}, not its Open's {}",
                authority.branch.target_tip_hash, open.target
            ));
        }
        if self
            .anchor
            .is_some_and(|anchor| authority.branch.anchor_hash != anchor)
        {
            found.push(format!(
                "has an owner anchored at {}, not the adopted anchor",
                authority.branch.anchor_hash
            ));
        }
        for detail in found {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: round {incarnation} {detail}"
            ));
        }
    }

    /// Judges the page round `incarnation` staged on `subscription`, which
    /// declared `ancestor` and carried `hashes`, against the fixture and the
    /// subscription's `Open`.
    fn path(
        &mut self,
        incarnation: u64,
        subscription: &Subscription,
        ancestor: Frontier,
        hashes: &[block::Hash],
    ) {
        let Some(open) = self.opens.get(subscription).cloned() else {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: round {incarnation} staged a page on session {} \
                 subscription {}, which the subscriber never opened",
                subscription.1, subscription.2
            ));
            return;
        };
        let expected = self.staged.get(subscription).copied();
        let declared_ok = match expected {
            Some(last) => ancestor == last,
            None => open.locators.contains(&ancestor.hash),
        };
        let mut found = Vec::new();
        if !declared_ok {
            found.push(format!(
                "declared ancestor {ancestor:?}, not {}",
                expected.map_or_else(
                    || "one of its Open's locators".to_owned(),
                    |last| format!("{last:?}")
                )
            ));
        }
        let mut parent = ancestor;
        for (index, hash) in hashes.iter().enumerate() {
            let height = block::Height(parent.height.0.saturating_add(1));
            if !self.children(parent).contains(hash) {
                found.push(format!(
                    "carried {hash} at height {}, which isn't a fixture child of {parent:?}",
                    height.0
                ));
                break;
            }
            if height > open.target_height {
                found.push(format!(
                    "carried {hash} at height {}, above its target's {}",
                    height.0, open.target_height.0
                ));
            } else if height == open.target_height
                && (*hash != open.target || index + 1 != hashes.len())
            {
                found.push(format!(
                    "carried {hash} at its target's height {} without ending at the target",
                    height.0
                ));
            }
            if *hash != open.target && open.locators.contains(hash) {
                found.push(format!("carried its Open's locator {hash}"));
            }
            parent = Frontier::new(height, *hash);
        }
        self.staged.insert(subscription.clone(), parent);
        for detail in found {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: round {incarnation}'s page {detail}"
            ));
        }
    }

    /// The fixture headers that can extend `parent`, one per branch.
    fn children(&self, parent: Frontier) -> [block::Hash; 3] {
        BRANCHES.map(|branch| self.fixture.extend(parent, 1, branch)[0].hash())
    }
}

#[cfg(test)]
mod tests;
