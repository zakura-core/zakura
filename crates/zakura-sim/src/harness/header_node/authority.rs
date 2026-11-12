//! `header-sync/commit-only-authorized`'s owner binding and path checks, on
//! the reactor's facts as its tap receives them.
//!
//! The model is the fixture: every header a run can hold is
//! [`HeaderFixture::extend`]'s child of its parent on one of the branches
//! the scenario's chains use, or one the scenario declares beyond it (an honest chain's invalid tip,
//! which its peer advertises and storage rejects at prepare), so a page's
//! headers are checked one by one as children of the header before, with
//! no list of chains. Content is storage's to judge, not this check's. Authority comes from what the node
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
//! The credit the subscriber builds is judged beside these, in [`credit`],
//! and the requests storage receives against their dispatches, in
//! [`submission`].
//!
//! The check applies once it judged a `Started`, a `PageStaged` or a credit
//! fact, or the
//! session view joined an admission's identity to its link or transport
//! row, so a run whose wrong identity starts no round still applies. A broken
//! rule is an [`AuthorityFinding`] naming its rule and subject, which the
//! tap records as a [`Claim::CommitOnlyAuthorized`](super::claim::Claim)
//! break at its drain.

use std::collections::{HashMap, HashSet};

use zakura_chain::block;
use zakura_header_chain::Frontier;
use zakura_network::zakura::{
    observe::{CreditKind, ReactorEvent},
    HeaderTargetPurpose, ZakuraPeerId,
};
use zakura_storage::header::fixture::HeaderFixture;

pub(crate) use finding::ledger_attribution;
use finding::{spell_frontier, spell_hash, spell_hashes};
pub use finding::{
    Attribution, AuthorityFinding, AuthorityRule, AuthoritySubject, CursorFact, EngineRunRef,
    ResetFact, UpdateKind,
};

use crate::observe::StorageFact;
use crate::stores::{debug_snapshot, EvidenceStores, Registered, Snapshot};

/// A subscription, by its peer, session and ID.
type Subscription = (ZakuraPeerId, u64, u64);

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

impl Snapshot for Store {
    /// Everything it holds.
    fn snapshot(&self) -> serde_json::Value {
        debug_snapshot(self)
    }
}

/// When the authority check first applied, shared by the tap and whoever
/// reads it. Cloning shares the store.
#[derive(Clone, Debug)]
pub struct AuthorityEvidence(Registered<Store>);

impl AuthorityEvidence {
    /// An empty store in `stores`, the node's.
    pub fn new(stores: &EvidenceStores) -> AuthorityEvidence {
        AuthorityEvidence(stores.register("authority", Store::default()))
    }

    /// Runs `read` on the store. Nothing else is called while the lock is
    /// held.
    fn with<R>(&self, read: impl FnOnce(&Store) -> R) -> R {
        self.0.read(read)
    }

    /// Runs `change` on the store, unless it's frozen, which drops the
    /// change.
    fn change<R>(&self, change: impl FnOnce(&mut Store) -> R) -> Option<R> {
        self.0.write(change)
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
    /// The fixture branches the scenario's chains use.
    branches: Vec<u8>,
    /// The headers the scenario declares beyond the fixture, by their
    /// parent's hash.
    declared: HashMap<block::Hash, Vec<block::Hash>>,
    /// The reactor's adopted finality anchor.
    anchor: Option<block::Hash>,
    /// Each subscription's `Open`.
    opens: HashMap<Subscription, Open>,
    /// The subscriptions a round started on.
    started: HashSet<Subscription>,
    /// Each subscription's last staged header.
    staged: HashMap<Subscription, Frontier>,
    credit: credit::Credit,
    submission: submission::Submission,
    contents: contents::Contents,
    breaks: Vec<AuthorityFinding>,
    evidence: AuthorityEvidence,
}

impl Authority {
    /// A check on `fixture`'s headers on `branches` and the `declared`
    /// ones beyond it, each a `(parent, header)` hash pair, that keeps when
    /// it applied in `evidence`.
    pub(super) fn new(
        evidence: AuthorityEvidence,
        fixture: HeaderFixture,
        branches: impl IntoIterator<Item = u8>,
        declared: impl IntoIterator<Item = (block::Hash, block::Hash)>,
    ) -> Authority {
        let mut branches: Vec<u8> = branches.into_iter().collect();
        branches.sort_unstable();
        branches.dedup();
        let mut children: HashMap<block::Hash, Vec<block::Hash>> = HashMap::new();
        for (parent, header) in declared {
            children.entry(parent).or_default().push(header);
        }
        Authority {
            fixture,
            branches,
            declared: children,
            anchor: None,
            opens: HashMap::new(),
            started: HashSet::new(),
            staged: HashMap::new(),
            credit: credit::Credit::default(),
            submission: submission::Submission::default(),
            contents: contents::Contents::default(),
            breaks: Vec::new(),
            evidence,
        }
    }

    /// Judges `event`, which the reactor's own task reported when
    /// `on_reactor`, in a stale call's handler when `stale_handler`, then
    /// notes what it says.
    pub(super) fn observe(&mut self, event: &ReactorEvent, on_reactor: bool, stale_handler: bool) {
        if self.credit.observe(event, on_reactor, stale_handler) {
            self.applies();
        }
        if !on_reactor {
            return;
        }
        if self.submission.observe(event) {
            self.applies();
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
                frame,
                ..
            } => {
                self.applies();
                let subscription = (request.peer.clone(), request.session_id, *subscription);
                self.path(
                    request.incarnation,
                    &subscription,
                    *ancestor,
                    hashes,
                    *frame,
                );
            }
            _ => {}
        }
    }

    /// The breaks found since the last call. Marks the check applicable at
    /// `step` if a fact since the last call made it so.
    pub(super) fn drain(&mut self, step: u64) -> Vec<AuthorityFinding> {
        self.evidence.change(|store| {
            if std::mem::take(&mut store.pending) {
                store.applicable.get_or_insert(step);
            }
        });
        let mut breaks = std::mem::take(&mut self.breaks);
        breaks.extend(self.credit.take_breaks());
        breaks.extend(self.submission.take_breaks());
        breaks.extend(self.contents.take_breaks());
        breaks
    }

    /// Notes one drain's storage `facts`, judging the submissions they
    /// make.
    pub(super) fn storage<'f>(&mut self, facts: impl IntoIterator<Item = &'f StorageFact>) {
        let facts: Vec<&StorageFact> = facts.into_iter().collect();
        for fact in &facts {
            self.credit.storage(fact);
            self.contents.storage(fact);
        }
        if self.submission.storage(facts) {
            self.applies();
        }
    }

    /// Judges one engine `run`, after the drain's storage facts, returning
    /// whether the commit-contents rules found it left the engine's durable
    /// state other than they allow. Each run makes the check applicable.
    pub(super) fn engine_run(
        &mut self,
        run: &zakura_storage::header::controlled::EngineRun,
    ) -> bool {
        self.applies();
        self.contents.run(run)
    }

    /// Notes that storage gave `hash` at `height` as a locator's entry, as
    /// a test feeds it.
    #[cfg(test)]
    fn note_read(&mut self, hash: block::Hash, height: block::Height) {
        self.credit.note_read(hash, height);
    }

    /// Notes that a fact made the check applicable.
    pub(super) fn applies(&self) {
        self.evidence.change(|store| store.pending = true);
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
        let subject = AuthoritySubject::round(incarnation, subscription);
        let Some(open) = self.opens.get(subscription) else {
            self.breaks.push(AuthorityFinding::new(
                AuthorityRule::OwnerBinding,
                subject,
                format!(
                    "round {incarnation} started on session {} subscription {}, which the \
                     subscriber never opened",
                    subscription.1, subscription.2
                ),
            ));
            return;
        };
        let mut found = Vec::new();
        if !self.started.contains(subscription) && locator != open.locators {
            found.push(format!(
                "handed on locator {}, not its Open's {}",
                spell_hashes(locator),
                spell_hashes(&open.locators)
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
                spell_hash(authority.branch.target_tip_hash),
                spell_hash(open.target)
            ));
        }
        if self
            .anchor
            .is_some_and(|anchor| authority.branch.anchor_hash != anchor)
        {
            found.push(format!(
                "has an owner anchored at {}, not the adopted anchor",
                spell_hash(authority.branch.anchor_hash)
            ));
        }
        for detail in found {
            self.breaks.push(AuthorityFinding::new(
                AuthorityRule::OwnerBinding,
                subject.clone(),
                format!("round {incarnation} {detail}"),
            ));
        }
    }

    /// Judges the page round `incarnation` staged on `subscription`, which
    /// came in `frame`, declared `ancestor` and carried `hashes`, against
    /// the fixture and the subscription's `Open`.
    fn path(
        &mut self,
        incarnation: u64,
        subscription: &Subscription,
        ancestor: Frontier,
        hashes: &[block::Hash],
        frame: Option<u64>,
    ) {
        let subject = AuthoritySubject::page(subscription, hashes, frame);
        let Some(open) = self.opens.get(subscription).cloned() else {
            self.breaks.push(AuthorityFinding::new(
                AuthorityRule::Path,
                subject,
                format!(
                    "round {incarnation} staged a page on session {} subscription {}, which the \
                     subscriber never opened",
                    subscription.1, subscription.2
                ),
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
                "declared ancestor {}, not {}",
                spell_frontier(ancestor),
                expected.map_or_else(|| "one of its Open's locators".to_owned(), spell_frontier)
            ));
        }
        let mut parent = ancestor;
        for (index, hash) in hashes.iter().enumerate() {
            let height = block::Height(parent.height.0.saturating_add(1));
            let spelled = spell_hash(*hash);
            if !self.children(parent).contains(hash) {
                found.push(format!(
                    "carried {spelled} at height {}, which isn't a fixture or declared child of {}",
                    height.0,
                    spell_frontier(parent)
                ));
                break;
            }
            if height > open.target_height {
                found.push(format!(
                    "carried {spelled} at height {}, above its target's {}",
                    height.0, open.target_height.0
                ));
            } else if height == open.target_height
                && (*hash != open.target || index + 1 != hashes.len())
            {
                found.push(format!(
                    "carried {spelled} at its target's height {} without ending at the target",
                    height.0
                ));
            }
            if *hash != open.target && open.locators.contains(hash) {
                found.push(format!("carried its Open's locator {spelled}"));
            }
            parent = Frontier::new(height, *hash);
        }
        self.staged.insert(subscription.clone(), parent);
        for detail in found {
            self.breaks.push(AuthorityFinding::new(
                AuthorityRule::Path,
                subject.clone(),
                format!("round {incarnation}'s page {detail}"),
            ));
        }
    }

    /// The headers that can extend `parent`: the fixture's, one per
    /// branch the scenario uses, and the scenario's declared ones.
    fn children(&self, parent: Frontier) -> Vec<block::Hash> {
        self.branches
            .iter()
            .map(|branch| self.fixture.extend(parent, 1, *branch)[0].hash())
            .chain(
                self.declared
                    .get(&parent.hash)
                    .into_iter()
                    .flatten()
                    .copied(),
            )
            .collect()
    }
}

mod contents;
mod credit;
mod finding;
mod submission;

#[cfg(test)]
pub(crate) mod tests;
