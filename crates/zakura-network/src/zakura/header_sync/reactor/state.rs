//! Logical header-sync reactor state.
//!
//! The types here hold plain values. They own no session, channel, future,
//! permit, or timer, and they read no clock. [`super::HeaderSyncReactor`]
//! owns those runtime resources and runs the operations this state records.

use std::collections::VecDeque;

use zakura_header_chain::HeaderWorkAuthority;
use zakura_rt::{collections::HashMap, time::Instant};

use super::{
    AdvertisedHeaderTarget, CachedPassBudget, CompletedHeaderTargets, PrunedPeer,
    RepairRequirementSlot, SchedulingOpportunity, Status, VctLocalOperation, VctRepairStall,
    ZakuraPeerId,
};

/// The header-sync reactor's scheduling records.
#[derive(Debug)]
pub(super) struct ReactorState {
    /// Scheduling facts for each admitted peer's current session.
    pub(super) peers: HashMap<ZakuraPeerId, PeerSchedule>,
    /// Direct continuation-locator reads in flight.
    pub(super) locators: LocatorQueries,
    /// Changes whenever an input can make a held-back ordinary offer eligible.
    pub(super) scheduling_revision: u64,
    pub(super) last_scheduling_opportunity: Option<SchedulingOpportunity>,
    pub(super) cached_pass_budget: CachedPassBudget,
    /// A changed key whose pass waits for [`Self::cached_pass_budget`] to refill.
    pub(super) cached_pass_deferred: bool,
    /// Deadlines until which the reactor refuses header-sync readmission to dropped peers.
    ///
    /// Each entry represents one admitted session that survived
    /// [`ZakuraHeaderSyncConfig::max_unproductive_header_requests`](super::ZakuraHeaderSyncConfig::max_unproductive_header_requests)
    /// request timeouts, or that sent headers failing validation.
    /// The reactor prunes expired entries during insertion and maintenance.
    pub(super) unproductive_peer_cooldowns: HashMap<ZakuraPeerId, Instant>,
    pub(super) request_deadlines: HashMap<ZakuraPeerId, Instant>,
    /// Sessions whose subscription this node closed, with the deadline for
    /// its terminal outcome. See [`super::HeaderSyncReactor::close_subscription`].
    pub(super) close_deadlines: HashMap<ZakuraPeerId, (u64, Instant)>,
    /// Sessions whose `Close` a full queue refused, with the session ID and
    /// when to try again. The time moves only after an actual attempt, so
    /// steady traffic cannot keep postponing the retry.
    pub(super) pending_closes: HashMap<ZakuraPeerId, (u64, Instant)>,
    /// What `HistoryPruned` taught the reactor about each peer. It outlives
    /// the peer's sessions, so reconnecting cannot reset its backoff, and
    /// maintenance drops entries once they no longer matter.
    pub(super) pruned_peers: HashMap<ZakuraPeerId, PrunedPeer>,
    pub(super) completed_targets: CompletedHeaderTargets,
    /// State version of the latest ordinary admission refused by a committed resource stall.
    pub(super) ordinary_resource_stall: Option<zakura_header_chain::StateVersion>,
    pub(super) vct_repair: RepairRequirementSlot,
    /// Escalation state for a repair generation that has not completed.
    pub(super) vct_repair_stall: Option<VctRepairStall>,
    /// Hard deadline for the one locally executing VCT repair mutation.
    pub(super) vct_local_operation: Option<VctLocalOperation>,
    /// Admitted authenticated peers in stable supplier selection order.
    ///
    /// The admitted peer limits bound this queue. New identities enter behind established peers.
    pub(super) vct_supplier_order: VecDeque<ZakuraPeerId>,
}

impl ReactorState {
    /// Empty state whose cached-pass budget is full at `now`.
    pub(super) fn new(now: Instant) -> Self {
        Self {
            peers: HashMap::new(),
            locators: LocatorQueries::default(),
            scheduling_revision: 0,
            last_scheduling_opportunity: None,
            cached_pass_budget: CachedPassBudget::new(now),
            cached_pass_deferred: false,
            unproductive_peer_cooldowns: HashMap::new(),
            request_deadlines: HashMap::new(),
            close_deadlines: HashMap::new(),
            pending_closes: HashMap::new(),
            pruned_peers: HashMap::new(),
            completed_targets: CompletedHeaderTargets::default(),
            ordinary_resource_stall: None,
            vct_repair: RepairRequirementSlot::default(),
            vct_repair_stall: None,
            vct_local_operation: None,
            vct_supplier_order: VecDeque::new(),
        }
    }

    /// Install fresh scheduling facts for `peer`'s new session, and return
    /// whether they replace an earlier session's facts.
    ///
    /// The replaced session's pending `Close` dies with it. The peer's pending
    /// locator read stays pending until its storage operation settles.
    pub(super) fn admit_session(&mut self, peer: &ZakuraPeerId) -> bool {
        self.close_deadlines.remove(peer);
        self.pending_closes.remove(peer);
        self.bump_scheduling_revision();
        self.peers
            .insert(peer.clone(), PeerSchedule::default())
            .is_some()
    }

    /// Remove `peer`'s session facts and its pending `Close`.
    pub(super) fn remove_session(&mut self, peer: &ZakuraPeerId) {
        if self.peers.remove(peer).is_some() {
            self.bump_scheduling_revision();
        }
        self.close_deadlines.remove(peer);
        self.pending_closes.remove(peer);
    }

    /// Record `status` as `peer`'s unspent offer.
    pub(super) fn record_status(&mut self, peer: &ZakuraPeerId, status: Status) {
        if let Some(schedule) = self.peers.get_mut(peer) {
            schedule.last_status = Some(status);
            schedule.status_spent = false;
        }
    }

    /// Spend `peer`'s offer.
    pub(super) fn spend_status(&mut self, peer: &ZakuraPeerId) {
        if let Some(schedule) = self.peers.get_mut(peer) {
            schedule.status_spent = true;
        }
    }

    /// Keep `peer`'s offer for a later pass, because a local blocker deferred its work.
    pub(super) fn retain_offer(&mut self, peer: &ZakuraPeerId) {
        self.bump_scheduling_revision();
        if let Some(schedule) = self.peers.get_mut(peer) {
            schedule.status_spent = false;
        }
    }

    pub(super) fn bump_scheduling_revision(&mut self) {
        self.scheduling_revision = self.scheduling_revision.wrapping_add(1);
    }
}

/// Scheduling facts for one admitted peer's current session.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct PeerSchedule {
    pub(super) last_status: Option<Status>,
    /// Whether `last_status` already had its one chance to start ordinary work.
    ///
    /// Staging spends a status, as does a status received during an ordinary attempt or
    /// one that cannot yield work under the current header authority. Fresh status or
    /// header authority restores it. Unstarted work deferred by local capacity, a committed
    /// stall, or repair preemption also keeps its offer until the blocker clears.
    /// Failed locator reads, sends, and ordinary attempts remain spent.
    pub(super) status_spent: bool,
    /// Consecutive requests this session answered with nothing usable.
    pub(super) unproductive_requests: u32,
}

/// The identity of one continuation-locator read.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct LocatorKey {
    /// The peer whose awaiting target the read serves.
    pub(super) peer: ZakuraPeerId,
    /// Distinguishes this read from every other read for `peer`.
    pub(super) query: u64,
}

/// How a locator request for one peer proceeds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum LocatorProposal {
    /// The peer's read in flight also serves this request.
    Joined,
    /// State recorded a new read, which IO must start or refuse.
    Start(LocatorKey),
}

/// What a settled locator read does to its peer's awaiting target.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum LocatorDisposition {
    /// No read with this key is pending, or the peer awaits no target.
    Stale,
    /// The read cannot serve the awaiting target, so the target needs a fresh read.
    Refetch,
    /// The read serves the awaiting target.
    Ready,
}

/// One pending continuation-locator read.
#[derive(Copy, Clone, Debug)]
struct LocatorQuery {
    query: u64,
    /// The authority whose selected path the read follows.
    scope: HeaderWorkAuthority,
}

/// Continuation-locator reads in flight, at most one per peer.
///
/// A peer's read stays pending until its storage operation settles, even when
/// the peer's session or awaiting target changes. A later request for the same
/// peer joins the pending read instead of starting another.
#[derive(Debug, Default)]
pub(super) struct LocatorQueries {
    pending: HashMap<ZakuraPeerId, LocatorQuery>,
    /// The query number of the next recorded read.
    next_query: u64,
}

impl LocatorQueries {
    /// The number of pending reads.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.pending.len()
    }

    /// Record a read for `peer` under `scope`, unless `peer` already has one pending.
    pub(super) fn propose(
        &mut self,
        peer: &ZakuraPeerId,
        scope: HeaderWorkAuthority,
    ) -> LocatorProposal {
        if self.pending.contains_key(peer) {
            return LocatorProposal::Joined;
        }
        let query = self.next_query;
        self.next_query = self.next_query.wrapping_add(1);
        self.pending
            .insert(peer.clone(), LocatorQuery { query, scope });
        LocatorProposal::Start(LocatorKey {
            peer: peer.clone(),
            query,
        })
    }

    /// Remove the read that `key` names, and return its authority.
    ///
    /// Another read for the same peer stays pending.
    pub(super) fn remove_exact(&mut self, key: &LocatorKey) -> Option<HeaderWorkAuthority> {
        let pending = self.pending.get(&key.peer)?;
        if pending.query != key.query {
            return None;
        }
        let scope = pending.scope;
        self.pending.remove(&key.peer);
        Some(scope)
    }

    /// Settle the read that `key` names, and classify its effect on `awaiting`,
    /// the peer's current awaiting target.
    ///
    /// Locators follow the local selected path, so tip churn under the same
    /// header generation and finality anchor can reuse one read, even across a
    /// session replacement. A generation or reanchor change needs a fresh read,
    /// and so does a failed read that belonged to a replaced tip.
    pub(super) fn settle(
        &mut self,
        key: &LocatorKey,
        awaiting: Option<&AdvertisedHeaderTarget>,
        found: bool,
    ) -> LocatorDisposition {
        let Some(query_scope) = self.remove_exact(key) else {
            return LocatorDisposition::Stale;
        };
        let Some(target) = awaiting else {
            return LocatorDisposition::Stale;
        };
        let same_selected_path_authority = target.scope.header_generation
            == query_scope.header_generation
            && target.scope.branch.anchor_hash == query_scope.branch.anchor_hash;
        let superseded_tip = target.status.selected_tip_hash != query_scope.branch.target_tip_hash;
        if !same_selected_path_authority || (!found && superseded_tip) {
            LocatorDisposition::Refetch
        } else {
            LocatorDisposition::Ready
        }
    }
}
