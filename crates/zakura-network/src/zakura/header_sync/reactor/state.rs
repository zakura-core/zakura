//! Logical header-sync reactor state.
//!
//! The types here hold plain values. They own no session, channel, future,
//! permit, or timer, and they read no clock. [`super::HeaderSyncReactor`]
//! owns those runtime resources and runs the operations this state records.

use std::{collections::VecDeque, time::Duration};

use zakura_chain::block;
use zakura_header_chain::HeaderWorkAuthority;
use zakura_rt::{collections::HashMap, time::Instant};

pub(super) use super::StorageCall;
pub use super::StorageCallKind;
use super::{
    source_id_from_peer, AdvertisedHeaderTarget, AuxSchema, CompletedHeaderTargets,
    HeaderPortOperation, HeaderRequestTerminal, HeaderTargetPhase, HeaderTargetPurpose,
    PeerWorkPriority, RepairRequirement, RepairRequirementSlot, Status, ZakuraPeerId,
    VCT_REPAIR_RETRY_INTERVAL,
};

/// The header-sync reactor's scheduling records.
#[derive(Debug)]
pub(super) struct ReactorState {
    /// Scheduling facts for each admitted peer's current session.
    pub(super) peers: HashMap<ZakuraPeerId, PeerSchedule>,
    /// Direct storage calls in flight, with their exact identities.
    pub(super) calls: StorageCalls,
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
            calls: StorageCalls::default(),
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
    /// A new peer joins the back of the repair supplier order. A replacement
    /// keeps its position until [`Self::requeue_supplier`] moves it.
    ///
    /// The replaced session's pending `Close` dies with it. The peer's pending
    /// locator read stays pending until its storage operation settles.
    pub(super) fn admit_session(&mut self, peer: &ZakuraPeerId) -> bool {
        self.close_deadlines.remove(peer);
        self.pending_closes.remove(peer);
        self.bump_scheduling_revision();
        let replaced = self
            .peers
            .insert(peer.clone(), PeerSchedule::default())
            .is_some();
        if !replaced {
            self.vct_supplier_order.push_back(peer.clone());
        }
        replaced
    }

    /// Remove `peer`'s session facts and its pending `Close`.
    pub(super) fn remove_session(&mut self, peer: &ZakuraPeerId) {
        if self.peers.remove(peer).is_some() {
            self.bump_scheduling_revision();
        }
        self.close_deadlines.remove(peer);
        self.pending_closes.remove(peer);
        self.vct_supplier_order.retain(|queued| queued != peer);
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

    /// Forget `key`'s read, which IO refused for lack of locator capacity, and
    /// keep the peer's offer for a later pass.
    pub(super) fn refuse_locator(&mut self, key: &LocatorKey) {
        let _ = self.locators.remove_exact(key);
        self.retain_offer(&key.peer);
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

    /// Restore the offer of every peer without work, because the header
    /// authority changed.
    pub(super) fn restore_idle_offers(&mut self, has_work: impl Fn(&ZakuraPeerId) -> bool) {
        for (peer, schedule) in &mut self.peers {
            if !has_work(peer) {
                schedule.status_spent = false;
            }
        }
    }

    /// Whether a full cached-offer pass may help, because `opportunity`
    /// differs from the key of the last pass.
    ///
    /// Only a pass refused by [`Self::take_cached_pass`] waits for a refill.
    /// Every other caller either has nothing to do or waits for its own
    /// wakeup, so this clears the refill wait first. A stale refill deadline
    /// would spin the reactor.
    pub(super) fn cached_pass_wanted(&mut self, opportunity: SchedulingOpportunity) -> bool {
        self.cached_pass_deferred = false;
        self.last_scheduling_opportunity != Some(opportunity)
    }

    /// Take one full cached-offer pass from the budget at `now`. A refused
    /// pass waits for the next refill, and its key stays uncached.
    pub(super) fn take_cached_pass(&mut self, now: Instant) -> bool {
        let taken = self.cached_pass_budget.try_take(now);
        if !taken {
            self.cached_pass_deferred = true;
        }
        taken
    }

    /// Finish the cached-offer pass for `opportunity` that ended at `now`.
    ///
    /// The pass keeps its pre-pass key: an external lease can drop while it
    /// scans, and a fresh key would swallow that release's wakeup.
    pub(super) fn finish_cached_pass(&mut self, opportunity: SchedulingOpportunity, now: Instant) {
        self.cached_pass_budget.finish_pass(now);
        self.last_scheduling_opportunity = Some(opportunity);
    }

    /// The unspent offers of peers without work, in cached-pass order: more
    /// claimed work first, then ascending peer bytes.
    ///
    /// `session_id` names each admitted peer's session.
    pub(super) fn cached_offers(
        &self,
        local: &zakura_header_chain::EngineSnapshot,
        has_work: impl Fn(&ZakuraPeerId) -> bool,
        session_id: impl Fn(&ZakuraPeerId) -> u64,
    ) -> Vec<(ZakuraPeerId, AdvertisedHeaderTarget)> {
        let mut offers: Vec<_> = self
            .peers
            .iter()
            .filter(|(peer, schedule)| !schedule.status_spent && !has_work(peer))
            .filter_map(|(peer, schedule)| {
                let status = schedule.last_status.as_ref()?;
                let target = AdvertisedHeaderTarget {
                    scope: HeaderWorkAuthority::for_target(local, status.selected_tip_hash),
                    session_id: session_id(peer),
                    status: status.clone(),
                };
                let priority = PeerWorkPriority::from_work_order(target.claimed_work_order(local));
                Some((priority, peer.clone(), target))
            })
            .collect();
        offers.sort_unstable_by(
            |(left_priority, left_peer, _), (right_priority, right_peer, _)| {
                right_priority
                    .cmp(left_priority)
                    .then_with(|| left_peer.as_bytes().cmp(right_peer.as_bytes()))
            },
        );
        offers
            .into_iter()
            .map(|(_, peer, target)| (peer, target))
            .collect()
    }

    /// Whether `peer`'s readmission cooldown still holds at `now`.
    pub(super) fn readmission_refused(&self, peer: &ZakuraPeerId, now: Instant) -> bool {
        self.unproductive_peer_cooldowns
            .get(peer)
            .is_some_and(|until| *until > now)
    }

    /// Refuse `peer`'s readmission for `cooldown` from `now`. A zero cooldown
    /// refuses nothing.
    pub(super) fn start_readmission_cooldown(
        &mut self,
        peer: &ZakuraPeerId,
        now: Instant,
        cooldown: Duration,
    ) {
        if !cooldown.is_zero() {
            let _ = self.prune_readmission_cooldowns(now);
            self.unproductive_peer_cooldowns
                .insert(peer.clone(), now + cooldown);
        }
    }

    /// Drop the cooldowns that end by `now`, and return whether any did.
    pub(super) fn prune_readmission_cooldowns(&mut self, now: Instant) -> bool {
        let before = self.unproductive_peer_cooldowns.len();
        self.unproductive_peer_cooldowns
            .retain(|_, until| *until > now);
        self.unproductive_peer_cooldowns.len() != before
    }

    /// The peers whose request deadline passed by `now`.
    pub(super) fn timed_out_requests(&self, now: Instant) -> Vec<ZakuraPeerId> {
        self.request_deadlines
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(peer, _)| peer.clone())
            .collect()
    }

    /// Record an attempt at `now` to close the subscription of `peer`'s
    /// session `session_id`.
    ///
    /// A refused `Close` waits [`CLOSE_RETRY_INTERVAL`] for a retry. A refused
    /// `Close` already waiting for this session keeps its retry time, so steady
    /// traffic cannot keep postponing it. A sent `Close` on a live
    /// subscription waits `outcome_timeout` for the terminal outcome, unless
    /// one already waits for this session.
    pub(super) fn record_close_attempt(
        &mut self,
        peer: &ZakuraPeerId,
        session_id: u64,
        attempt: CloseAttempt,
        now: Instant,
        outcome_timeout: Duration,
    ) {
        let CloseAttempt::Sent { live } = attempt else {
            if self
                .pending_closes
                .get(peer)
                .is_none_or(|(pending, _)| *pending != session_id)
            {
                self.pending_closes
                    .insert(peer.clone(), (session_id, now + CLOSE_RETRY_INTERVAL));
            }
            return;
        };
        self.pending_closes.remove(peer);
        if !live {
            return;
        }
        if self
            .close_deadlines
            .get(peer)
            .is_none_or(|(closing, _)| *closing != session_id)
        {
            self.close_deadlines
                .insert(peer.clone(), (session_id, now + outcome_timeout));
        }
    }

    /// Remove and return each refused `Close` due for a retry by `now`, with
    /// its session ID.
    pub(super) fn take_due_closes(&mut self, now: Instant) -> Vec<(ZakuraPeerId, u64)> {
        take_due(&mut self.pending_closes, now)
    }

    /// Remove and return each sent `Close` whose terminal outcome was due by
    /// `now`, with its session ID.
    pub(super) fn take_expired_closes(&mut self, now: Instant) -> Vec<(ZakuraPeerId, u64)> {
        take_due(&mut self.close_deadlines, now)
    }

    /// The schema for an ordinary round of up to `count` headers after `start`
    /// from `peer`, whose status is `status`, at `now`: `schema` while the
    /// round ends at or below the peer's work anchor and no `HistoryPruned`
    /// fallback holds, and no records otherwise.
    ///
    /// A publisher is sure to hold records only through its finalized height,
    /// and a schema 1 page waits for a missing record rather than dropping it,
    /// so a round past the anchor could stall. A pruned publisher lacks them
    /// below its index height too, which the fallback learns.
    pub(super) fn ordinary_schema(
        &self,
        peer: &ZakuraPeerId,
        schema: AuxSchema,
        status: &Status,
        start: block::Height,
        count: u32,
        now: Instant,
    ) -> AuxSchema {
        let fallback = self
            .pruned_peers
            .get(peer)
            .and_then(|pruned| pruned.aux_fallback)
            .is_some_and(|fallback| fallback.until > now);
        if !fallback && start.0.saturating_add(count) <= status.work_anchor_height.0 {
            schema
        } else {
            AuxSchema::None
        }
    }

    /// Whether ordinary discovery with `peer` still waits at `now` after a
    /// round without records ended `HistoryPruned` at the selected header tip
    /// `tip`.
    pub(super) fn pruned_pause_holds(
        &self,
        peer: &ZakuraPeerId,
        tip: zakura_header_chain::Frontier,
        now: Instant,
    ) -> bool {
        self.pruned_peers
            .get(peer)
            .and_then(|pruned| pruned.pruned_pause)
            .is_some_and(|(pause, paused_at)| pause.until > now && paused_at == tip)
    }

    /// Remember that `peer` ended an ordinary round asking for `schema` with
    /// `HistoryPruned` at `now`, while the selected header tip was `tip`.
    /// This is legal and never a fault.
    ///
    /// After a schema 1 round, the peer's next rounds ask for no records,
    /// starting at once from the validated tip, and schema 1 is probed again
    /// after a backoff. After a round without records, ordinary discovery with
    /// the peer pauses instead, so another peer takes the target.
    ///
    /// Return whether the peer's rounds now fall back to no records.
    pub(super) fn note_history_pruned(
        &mut self,
        peer: &ZakuraPeerId,
        schema: AuxSchema,
        tip: Option<zakura_header_chain::Frontier>,
        now: Instant,
    ) -> bool {
        let Some(schedule) = self.peers.get_mut(peer) else {
            return false;
        };
        let pruned = self.pruned_peers.entry(peer.clone()).or_default();
        match schema {
            AuxSchema::V1 => {
                pruned.aux_fallback = Some(PrunedBackoff::after(pruned.aux_fallback, now));
                schedule.status_spent = false;
                self.bump_scheduling_revision();
                true
            }
            AuxSchema::None => {
                if let Some(tip) = tip {
                    let previous = pruned.pruned_pause.map(|(pause, _)| pause);
                    pruned.pruned_pause = Some((PrunedBackoff::after(previous, now), tip));
                }
                false
            }
        }
    }

    /// End `peer`'s `HistoryPruned` pause after an admitted ordinary round,
    /// and its schema fallback when the round asked for `schema` 1.
    pub(super) fn note_ordinary_round_admitted(&mut self, peer: &ZakuraPeerId, schema: AuxSchema) {
        let Some(pruned) = self.pruned_peers.get_mut(peer) else {
            return;
        };
        pruned.pruned_pause = None;
        if schema == AuxSchema::V1 {
            pruned.aux_fallback = None;
        }
        if *pruned == PrunedPeer::default() {
            self.pruned_peers.remove(peer);
        }
    }

    /// Drop what `HistoryPruned` taught about peers once it no longer matters at `now`.
    pub(super) fn prune_pruned_peers(&mut self, now: Instant) {
        self.pruned_peers.retain(|_, pruned| pruned.relevant(now));
    }

    /// Hold back ordinary discovery until the committed state version passes
    /// `state_version`, where state refused an admission for local resources.
    pub(super) fn note_resource_stall(&mut self, state_version: zakura_header_chain::StateVersion) {
        self.ordinary_resource_stall = self.ordinary_resource_stall.max(Some(state_version));
    }

    /// Whether a committed resource stall still holds back new ordinary
    /// attempts under the committed state version `committed`.
    ///
    /// Every supplier would hit the same local stall, so discovery waits until
    /// the committed state version advances past the stall receipt.
    pub(super) fn ordinary_discovery_paused(
        &self,
        committed: Option<zakura_header_chain::StateVersion>,
    ) -> bool {
        self.ordinary_resource_stall
            .is_some_and(|stalled_at| committed.is_none_or(|committed| committed <= stalled_at))
    }

    /// Arm `peer`'s request deadline `timeout` after `now`.
    pub(super) fn arm_request_deadline(
        &mut self,
        peer: ZakuraPeerId,
        now: Instant,
        timeout: Duration,
    ) {
        self.request_deadlines.insert(peer, now + timeout);
    }

    pub(super) fn clear_request_deadline(&mut self, peer: &ZakuraPeerId) {
        self.request_deadlines.remove(peer);
    }

    /// Forget the `Close` this node sent to `peer`'s `session_id` once that session's
    /// subscription ends.
    pub(super) fn settle_close(&mut self, peer: &ZakuraPeerId, session_id: u64) {
        if self
            .close_deadlines
            .get(peer)
            .is_some_and(|(closing, _)| *closing == session_id)
        {
            self.close_deadlines.remove(peer);
        }
    }

    /// Charge one unproductive request to `peer`'s current session.
    ///
    /// Returns whether the session reached `limit`. A zero `limit` never drops a session.
    pub(super) fn charge_unproductive_request(&mut self, peer: &ZakuraPeerId, limit: u32) -> bool {
        let schedule = self
            .peers
            .get_mut(peer)
            .expect("sessions and schedules share their keys");
        schedule.unproductive_requests = schedule.unproductive_requests.saturating_add(1);
        limit != 0 && schedule.unproductive_requests >= limit
    }

    pub(super) fn reset_unproductive_requests(&mut self, peer: &ZakuraPeerId) {
        if let Some(schedule) = self.peers.get_mut(peer) {
            schedule.unproductive_requests = 0;
        }
    }

    /// Move the supplier whose session `source` names to the back of the repair supplier order.
    pub(super) fn rotate_supplier(&mut self, source: zakura_header_chain::SourceId) {
        let Some(index) = self
            .vct_supplier_order
            .iter()
            .position(|peer| source_id_from_peer(peer) == source)
        else {
            return;
        };
        let peer = self
            .vct_supplier_order
            .remove(index)
            .expect("the supplier index came from this bounded queue");
        self.vct_supplier_order.push_back(peer);
    }

    /// Forget a replaced session's repair history and move it to the back of the supplier order.
    pub(super) fn requeue_supplier(&mut self, source: zakura_header_chain::SourceId) {
        if let Some(task) = self.vct_repair.current_mut() {
            task.forget_source(source);
        }
        self.rotate_supplier(source);
    }

    /// Apply `retry`'s attribution to `owner`'s repair and report whether a retry is scheduled.
    ///
    /// Supplier and excluded-input failures charge the supplier and rotate it to the back
    /// of the supplier order. A local failure backs the repair off until
    /// [`VCT_REPAIR_RETRY_INTERVAL`] after `now`. A stale episode schedules nothing.
    /// When the supplier's session is no longer current, the departed session's failure
    /// history leaves the task.
    pub(super) fn schedule_repair_retry(
        &mut self,
        owner: zakura_header_chain::BodyWorkOwner,
        retry: VctRepairRetry,
        session_is_current: bool,
        now: Instant,
    ) -> bool {
        let source = retry.source;
        let scheduled =
            self.vct_repair
                .get_mut(owner)
                .is_some_and(|task| match retry.attribution {
                    VctRepairRetryAttribution::Supplier => {
                        if retry.terminal == HeaderRequestTerminal::TargetNotSelected {
                            task.wait_for_supplier_status(source).is_ok()
                        } else {
                            task.retry(source).is_ok()
                        }
                    }
                    VctRepairRetryAttribution::ExcludedInput => task.exclude_input(source).is_ok(),
                    VctRepairRetryAttribution::Stale => false,
                    VctRepairRetryAttribution::Local => task
                        .defer_local_retry_until(now + VCT_REPAIR_RETRY_INTERVAL)
                        .is_ok(),
                });
        if !scheduled {
            return false;
        }
        if !session_is_current {
            // Retained local work can finish after its supplier reconnects.
            // Its failure history belongs to the departed session.
            if let Some(task) = self.vct_repair.get_mut(owner) {
                task.forget_source(source);
            }
        }
        if retry.attribution.charges_supplier() {
            self.rotate_supplier(source);
        }
        true
    }

    /// Record the latest reason repair `generation` has not completed.
    ///
    /// A new generation restarts the stall clock at `now`.
    pub(super) fn record_repair_stall(
        &mut self,
        generation: u64,
        predecessor: zakura_header_chain::Frontier,
        rejections: VctSupplierRejections,
        outcome: VctRepairStallOutcome,
        now: Instant,
    ) {
        match self.vct_repair_stall.as_mut() {
            Some(record) if record.generation == generation => {
                record.predecessor = predecessor;
                record.rejections = rejections;
                record.outcome = outcome;
            }
            _ => {
                self.vct_repair_stall = Some(VctRepairStall {
                    generation,
                    since: now,
                    last_trace: None,
                    reported: false,
                    predecessor,
                    rejections,
                    outcome,
                });
            }
        }
    }

    /// Return the stall output due at `now`, and mark it emitted.
    ///
    /// The stall record dies with its generation. At most one trace row is due per
    /// [`VCT_REPAIR_STALL_TRACE_INTERVAL`], and at most one error report per generation.
    pub(super) fn refresh_repair_stall(&mut self, now: Instant) -> Option<RepairStallDue> {
        let record = self.vct_repair_stall?;
        let Some(task) = self
            .vct_repair
            .current()
            .filter(|task| task.repair_generation == record.generation)
            .cloned()
        else {
            self.vct_repair_stall = None;
            return None;
        };
        let stalled_for = now.saturating_duration_since(record.since);
        let trace = record.last_trace.is_none_or(|last| {
            now.saturating_duration_since(last) >= VCT_REPAIR_STALL_TRACE_INTERVAL
        });
        let report = !record.reported && stalled_for >= VCT_REPAIR_STALL_REPORT_AFTER;
        if !trace && !report {
            return None;
        }
        let best_peer_tip = self
            .peers
            .values()
            .filter_map(|schedule| schedule.last_status.as_ref())
            .map(|status| (status.selected_tip_height, status.selected_tip_hash))
            .max_by_key(|(height, _)| *height);
        if let Some(current) = self.vct_repair_stall.as_mut() {
            if trace {
                current.last_trace = Some(now);
            }
            if report {
                current.reported = true;
            }
        }
        Some(RepairStallDue {
            task,
            record,
            trace,
            report,
            stalled_for,
            best_peer_tip,
        })
    }

    /// Mark `owner`'s repair applied, rotate its supplier, and end its stall record.
    pub(super) fn complete_repair(
        &mut self,
        owner: zakura_header_chain::BodyWorkOwner,
        source: zakura_header_chain::SourceId,
    ) {
        let _ = self
            .vct_repair
            .get_mut(owner)
            .expect("the admitted repair remains owned by its active request")
            .complete();
        self.rotate_supplier(source);
        self.vct_repair_stall = None;
    }

    /// Whether `desired` is the repair the reactor already runs.
    pub(super) fn preserves_repair(&self, desired: &RepairRequirement) -> bool {
        self.vct_repair.current().is_some_and(|task| {
            task.repair_generation == desired.repair_generation
                && task.height == desired.height
                && task.owner.header_generation == desired.owner.header_generation
                && task.owner.verified_generation == desired.owner.verified_generation
                && task.owner.branch == desired.owner.branch
        })
    }

    /// Remove the current repair and its stall record.
    pub(super) fn retire_repair(&mut self) -> Option<RepairRequirement> {
        self.vct_repair_stall = None;
        self.vct_repair.take()
    }

    /// Install `desired` as the sole repair.
    pub(super) fn install_repair(&mut self, desired: RepairRequirement) {
        let replaced = self.vct_repair.insert(desired);
        debug_assert!(
            replaced.is_none(),
            "the sole repair is cleared before scheduling its replacement"
        );
    }

    /// Clear the local operation that `owner` finished in `phase`.
    ///
    /// An operation already reported as fatal stays installed until shutdown.
    pub(super) fn finish_local_operation(
        &mut self,
        owner: zakura_header_chain::HeaderSyncWorkOwner,
        phase: HeaderTargetPhase,
    ) {
        if self.vct_local_operation.as_ref().is_some_and(|operation| {
            operation.owner == owner && operation.phase == phase && !operation.fatal_sent
        }) {
            self.vct_local_operation = None;
        }
    }

    /// Check the local operation's hard deadline at `now`.
    ///
    /// An expired operation is marked fatal once, and stays installed.
    pub(super) fn expire_local_operation(&mut self, now: Instant) -> LocalOperationExpiry {
        let Some(operation) = self.vct_local_operation.as_mut() else {
            return LocalOperationExpiry::Idle;
        };
        if operation.fatal_sent {
            return LocalOperationExpiry::AlreadyFatal;
        }
        if operation.deadline > now {
            return LocalOperationExpiry::Running;
        }
        operation.fatal_sent = true;
        LocalOperationExpiry::Expired(operation.clone())
    }

    /// The earliest deadline this state waits for, if any.
    pub(super) fn next_deadline(&self) -> Option<Instant> {
        self.request_deadlines
            .values()
            .copied()
            .chain(self.close_deadlines.values().map(|(_, deadline)| *deadline))
            .chain(self.pending_closes.values().map(|(_, retry_at)| *retry_at))
            .chain(
                self.vct_repair
                    .current()
                    .and_then(RepairRequirement::next_deadline),
            )
            .chain(self.vct_repair_stall.map(VctRepairStall::next_deadline))
            .chain(
                self.cached_pass_deferred
                    .then(|| self.cached_pass_budget.next_refill()),
            )
            .chain(
                self.vct_local_operation
                    .as_ref()
                    .filter(|operation| !operation.fatal_sent)
                    .map(|operation| operation.deadline),
            )
            .min()
    }
}

/// How an attempt to send `Close` on a session ended.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum CloseAttempt {
    /// The session's full queue refused the `Close`.
    Refused,
    /// The `Close` entered the queue, or the session could not take it at all.
    Sent {
        /// Whether a subscription was live when the reactor tried to close it.
        live: bool,
    },
}

/// Remove and return each `(session_id, deadline)` entry due by `now`.
fn take_due(
    entries: &mut HashMap<ZakuraPeerId, (u64, Instant)>,
    now: Instant,
) -> Vec<(ZakuraPeerId, u64)> {
    let due: Vec<_> = entries
        .iter()
        .filter(|(_, (_, deadline))| *deadline <= now)
        .map(|(peer, (session_id, _))| (peer.clone(), *session_id))
        .collect();
    for (peer, _) in &due {
        entries.remove(peer);
    }
    due
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

impl StorageCallKind {
    /// The kind of storage call `action` makes, or `None` for an action that
    /// the reactor only records.
    pub(super) fn of(action: &HeaderPortOperation) -> Option<Self> {
        match action {
            HeaderPortOperation::QueryHeaderLocator { .. } => Some(Self::Locator),
            HeaderPortOperation::QueryVctRepairContext { .. } => Some(Self::VctRepairContext),
            HeaderPortOperation::PrepareHeaderTarget { .. } => Some(Self::Prepare),
            HeaderPortOperation::ApplyHeaderTarget { .. } => Some(Self::Apply),
            HeaderPortOperation::Misbehavior { .. } | HeaderPortOperation::DropPeer { .. } => None,
        }
    }
}

/// Why a settlement names no accepted call.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct UnknownStorageCall;

/// Accepted direct storage calls that have not settled.
///
/// Each call settles once, and only its exact number and kind settle it. The
/// reactor pushes one future per accepted call, so its pending futures bound
/// this map.
#[derive(Debug, Default)]
pub(super) struct StorageCalls {
    last: u64,
    pending: HashMap<u64, StorageCallKind>,
}

impl StorageCalls {
    /// Number a new call of `kind`. The call has not started.
    pub(super) fn propose(&mut self, kind: StorageCallKind) -> StorageCall {
        self.last = self.last.wrapping_add(1);
        StorageCall {
            number: self.last,
            kind,
        }
    }

    /// Record that `call` started.
    pub(super) fn accept(&mut self, call: StorageCall) {
        self.pending.insert(call.number, call.kind);
    }

    /// Settle exactly `call`.
    pub(super) fn settle(&mut self, call: StorageCall) -> Result<(), UnknownStorageCall> {
        match self.pending.get(&call.number) {
            Some(kind) if *kind == call.kind => {
                self.pending.remove(&call.number);
                Ok(())
            }
            _ => Err(UnknownStorageCall),
        }
    }

    /// Accepted calls that have not settled, by ascending number.
    #[cfg(any(test, feature = "p2p-fuzz"))]
    pub(super) fn pending(&self) -> Vec<StorageCall> {
        let mut pending: Vec<_> = self
            .pending
            .iter()
            .map(|(number, kind)| StorageCall {
                number: *number,
                kind: *kind,
            })
            .collect();
        pending.sort_unstable_by_key(|call| call.number);
        pending
    }
}

/// The identity of one continuation-locator read.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct LocatorKey {
    /// The peer whose awaiting target the read serves.
    pub(super) peer: ZakuraPeerId,
    /// The storage call that performs this read.
    pub(super) call: StorageCall,
}

/// How a locator request for one peer proceeds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum LocatorProposal {
    /// The peer's read in flight, this exact call, also serves this request.
    Joined(StorageCall),
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
    call: StorageCall,
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
}

impl LocatorQueries {
    /// The read pending for `peer`, with its call and authority.
    #[cfg(any(test, feature = "p2p-fuzz"))]
    pub(super) fn pending_read(
        &self,
        peer: &ZakuraPeerId,
    ) -> Option<(StorageCall, HeaderWorkAuthority)> {
        self.pending
            .get(peer)
            .map(|query| (query.call, query.scope))
    }

    /// Whether a read for `peer` is pending.
    #[cfg(any(test, feature = "p2p-fuzz"))]
    pub(super) fn contains(&self, peer: &ZakuraPeerId) -> bool {
        self.pending.contains_key(peer)
    }

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
        calls: &mut StorageCalls,
    ) -> LocatorProposal {
        if let Some(pending) = self.pending.get(peer) {
            return LocatorProposal::Joined(pending.call);
        }
        let call = calls.propose(StorageCallKind::Locator);
        self.pending
            .insert(peer.clone(), LocatorQuery { call, scope });
        LocatorProposal::Start(LocatorKey {
            peer: peer.clone(),
            call,
        })
    }

    /// Remove the read that `key` names, and return its authority.
    ///
    /// Another read for the same peer stays pending.
    pub(super) fn remove_exact(&mut self, key: &LocatorKey) -> Option<HeaderWorkAuthority> {
        let pending = self.pending.get(&key.peer)?;
        if pending.call != key.call {
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

/// Inputs that can make a previously blocked ordinary offer eligible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SchedulingOpportunity {
    pub(super) policy_revision: u64,
    pub(super) queue_revision: u64,
    pub(super) claimed_headers: usize,
    pub(super) available_locators: usize,
    pub(super) resource_stalled: bool,
}

/// Full cached-offer passes the reactor may run back to back.
///
/// A pass clones and sorts every idle offer. One peer whose statuses keep
/// changing its own unstarted target changes the scheduling key on every
/// message, so without a budget it could buy one such pass per message.
/// Past the burst, passes refill one per [`CACHED_PASS_REFILL`].
pub(super) const CACHED_PASS_BURST: u32 = 32;

/// The interval that refills one full cached-offer pass.
pub(super) const CACHED_PASS_REFILL: std::time::Duration = std::time::Duration::from_millis(1);

/// A token bucket of full cached-offer passes; see [`CACHED_PASS_BURST`].
#[derive(Clone, Copy, Debug)]
pub(super) struct CachedPassBudget {
    pub(super) passes: u32,
    pub(super) refilled_at: Instant,
}

impl CachedPassBudget {
    /// A full bucket at `now`.
    pub(super) fn new(now: Instant) -> Self {
        Self {
            passes: CACHED_PASS_BURST,
            refilled_at: now,
        }
    }

    /// Refill for the time since the last refill, then take one pass if any
    /// is left.
    pub(super) fn try_take(&mut self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.refilled_at);
        let refills = elapsed.as_nanos() / CACHED_PASS_REFILL.as_nanos();
        if refills > 0 {
            // At most the burst refills count, which fits u32.
            let refills = u32::try_from(refills.min(u128::from(CACHED_PASS_BURST)))
                .expect("the burst fits u32");
            self.passes = self.passes.saturating_add(refills).min(CACHED_PASS_BURST);
            self.refilled_at = if self.passes == CACHED_PASS_BURST {
                now
            } else {
                self.refilled_at + CACHED_PASS_REFILL * refills
            };
        }
        let Some(passes) = self.passes.checked_sub(1) else {
            return false;
        };
        self.passes = passes;
        true
    }

    /// Restart the refill clock when a pass ends at `now`.
    ///
    /// Time spent scanning earns no refill. Otherwise a pass that takes a whole
    /// refill interval would pay for the next one, and passes could run back to
    /// back.
    pub(super) fn finish_pass(&mut self, now: Instant) {
        self.refilled_at = self.refilled_at.max(now);
    }

    /// When the next pass refills.
    pub(super) fn next_refill(&self) -> Instant {
        self.refilled_at + CACHED_PASS_REFILL
    }
}

/// How often a `Close` that a session's full queue refused is tried again.
pub(super) const CLOSE_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// The first wait after an ordinary round ends `HistoryPruned`. Each repeat
/// before a success doubles it, up to [`PRUNED_BACKOFF_MAX`].
pub(super) const PRUNED_BACKOFF_MIN: std::time::Duration = std::time::Duration::from_secs(60);

/// The longest wait after repeated `HistoryPruned` rounds with one peer.
pub(super) const PRUNED_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(16 * 60);

/// What `HistoryPruned` taught the reactor about one peer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct PrunedPeer {
    /// Set when an ordinary schema 1 round with this peer ended
    /// `HistoryPruned`, because a pruned publisher lacks the records. Ordinary
    /// rounds ask for no records until it expires, then probe schema 1 once.
    /// An admitted schema 1 round clears it.
    pub(super) aux_fallback: Option<PrunedBackoff>,
    /// Set when an ordinary round without records also ended
    /// `HistoryPruned`. Ordinary discovery with this peer waits until it
    /// expires or the selected header tip it names moves. An admitted round
    /// clears it.
    pub(super) pruned_pause: Option<(PrunedBackoff, zakura_header_chain::Frontier)>,
}

impl PrunedPeer {
    /// Whether either wait still matters at `now`. An expired fallback stays
    /// for one more maximum backoff, so a failed probe soon after still
    /// doubles its delay.
    pub(super) fn relevant(&self, now: Instant) -> bool {
        self.aux_fallback
            .is_some_and(|fallback| fallback.until + PRUNED_BACKOFF_MAX > now)
            || self
                .pruned_pause
                .is_some_and(|(pause, _)| pause.until > now)
    }
}

/// A wait that doubles each time it is renewed, until a success clears it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PrunedBackoff {
    pub(super) until: Instant,
    pub(super) delay: std::time::Duration,
}

impl PrunedBackoff {
    /// The wait from `now` after `previous`: [`PRUNED_BACKOFF_MIN`] at first,
    /// then twice the previous delay, at most [`PRUNED_BACKOFF_MAX`].
    pub(super) fn after(previous: Option<Self>, now: Instant) -> Self {
        let delay = previous.map_or(PRUNED_BACKOFF_MIN, |previous| {
            previous.delay.saturating_mul(2).min(PRUNED_BACKOFF_MAX)
        });
        Self {
            until: now + delay,
            delay,
        }
    }
}

/// Hard deadline for one VCT repair prepare or apply operation.
pub(super) const VCT_LOCAL_OPERATION_FATAL_AFTER: std::time::Duration =
    std::time::Duration::from_secs(30 * 60);

/// Minimum interval between repeated stalled-repair trace rows for one repair generation.
///
/// The reactor emits the first row immediately, then samples while nothing changes. The bound
/// keeps a long stalled repair from dominating the header-sync JSONL trace.
pub(super) const VCT_REPAIR_STALL_TRACE_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(10);

/// Time one repair generation may stay stalled before the reactor reports it at error level.
pub(super) const VCT_REPAIR_STALL_REPORT_AFTER: std::time::Duration =
    std::time::Duration::from_secs(60);

/// Why each advertised peer failed to qualify as a VCT repair supplier.
///
/// The reactor reports the tally when no supplier qualifies. Without it the trace records only
/// that the repair made no progress, not which requirement excluded the network.
#[derive(Copy, Clone, Debug, Default)]
pub(super) struct VctSupplierRejections {
    /// Peers that had advertised a status.
    pub(super) considered: u64,
    /// Peers whose selected tip is below the repair target.
    pub(super) below_target_height: u64,
    /// Peers that advertise no capacity for the one-header repair response.
    pub(super) insufficient_capacity: u64,
    /// Peers that do not support the V1 auxiliary schema.
    pub(super) unsupported_schema: u64,
    /// Peers already serving another header request.
    pub(super) already_serving: u64,
    /// Peers already tried in the current durable episode.
    pub(super) already_tried: u64,
    /// Peers waiting for a later status after a temporary Busy response.
    pub(super) supplier_status_wait: u64,
    /// Peers that already supplied a retained rooted payload for this target.
    pub(super) retained_source: u64,
    /// Peers that already returned semantic input excluded by this episode.
    pub(super) excluded_input: u64,
    /// Eligible peers whose request could not enter the ordered send queue.
    pub(super) send_failed: u64,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum VctRepairStallOutcome {
    LocalCapacityUnavailable,
    LocalFailure,
    LocalOperationPending,
    LocalSendFailure,
    NoEligibleSupplier,
    SupplierFailure,
    SupplierSendFailure,
}

impl VctRepairStallOutcome {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::LocalCapacityUnavailable => "local_capacity_unavailable",
            Self::LocalFailure => "local_failure",
            Self::LocalOperationPending => "local_operation_pending",
            Self::LocalSendFailure => "local_send_failed",
            Self::NoEligibleSupplier => "no_eligible_supplier",
            Self::SupplierFailure => "supplier_failure",
            Self::SupplierSendFailure => "supplier_send_failed",
        }
    }

    pub(super) fn attribution(self) -> &'static str {
        match self {
            Self::LocalCapacityUnavailable
            | Self::LocalFailure
            | Self::LocalOperationPending
            | Self::LocalSendFailure => "local",
            Self::NoEligibleSupplier | Self::SupplierFailure | Self::SupplierSendFailure => {
                "supplier"
            }
        }
    }
}

/// One repair generation that has not completed after its first failed attempt.
#[derive(Copy, Clone, Debug)]
pub(super) struct VctRepairStall {
    /// Repair generation this record tracks.
    pub(super) generation: u64,
    /// First failed attempt for this generation.
    pub(super) since: Instant,
    /// Last emitted trace row, or none before the immediate first row.
    pub(super) last_trace: Option<Instant>,
    /// Whether the reactor already reported this generation at error level.
    pub(super) reported: bool,
    /// Latest exact predecessor for diagnostic output.
    pub(super) predecessor: zakura_header_chain::Frontier,
    /// Latest supplier rejection tally.
    pub(super) rejections: VctSupplierRejections,
    /// Latest local or supplier outcome that kept the repair stalled.
    pub(super) outcome: VctRepairStallOutcome,
}

impl VctRepairStall {
    pub(super) fn next_deadline(self) -> Instant {
        let trace_at = self
            .last_trace
            .map_or(self.since, |last| last + VCT_REPAIR_STALL_TRACE_INTERVAL);
        if self.reported {
            trace_at
        } else {
            trace_at.min(self.since + VCT_REPAIR_STALL_REPORT_AFTER)
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum VctRepairRetryAttribution {
    Supplier,
    ExcludedInput,
    Stale,
    Local,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct VctRepairRetry {
    pub(super) source: zakura_header_chain::SourceId,
    pub(super) terminal: HeaderRequestTerminal,
    pub(super) attribution: VctRepairRetryAttribution,
}

impl VctRepairRetryAttribution {
    /// Whether the failure charges the supplier and rotates it in the supplier order.
    pub(super) fn charges_supplier(self) -> bool {
        matches!(self, Self::Supplier | Self::ExcludedInput)
    }

    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Supplier => "supplier_retry",
            Self::ExcludedInput => "excluded_input",
            Self::Stale => "stale_episode",
            Self::Local => "local_backoff",
        }
    }

    pub(super) fn stall_outcome(self) -> VctRepairStallOutcome {
        match self {
            Self::Supplier | Self::ExcludedInput => VctRepairStallOutcome::SupplierFailure,
            Self::Stale | Self::Local => VctRepairStallOutcome::LocalFailure,
        }
    }
}

impl VctRepairRetry {
    pub(super) fn supplier(
        source: zakura_header_chain::SourceId,
        terminal: HeaderRequestTerminal,
    ) -> Self {
        Self {
            source,
            terminal,
            attribution: VctRepairRetryAttribution::Supplier,
        }
    }

    pub(super) fn local(
        source: zakura_header_chain::SourceId,
        terminal: HeaderRequestTerminal,
    ) -> Self {
        Self {
            source,
            terminal,
            attribution: VctRepairRetryAttribution::Local,
        }
    }

    pub(super) fn excluded_input(source: zakura_header_chain::SourceId) -> Self {
        Self {
            source,
            terminal: HeaderRequestTerminal::ExcludedAuxiliaryInput,
            attribution: VctRepairRetryAttribution::ExcludedInput,
        }
    }

    pub(super) fn stale(source: zakura_header_chain::SourceId) -> Self {
        Self {
            source,
            terminal: HeaderRequestTerminal::RepairObsolete,
            attribution: VctRepairRetryAttribution::Stale,
        }
    }
}

pub(super) fn vct_error_retry(
    source: zakura_header_chain::SourceId,
    terminal: HeaderRequestTerminal,
    error: &zakura_header_chain::HeaderChainError,
) -> VctRepairRetry {
    if error.category == zakura_header_chain::ErrorCategory::StaleTargetOrGeneration {
        return VctRepairRetry::stale(source);
    }
    let attributed_source = match error.attribution {
        zakura_header_chain::Attribution::HeaderPeer(attributed)
        | zakura_header_chain::Attribution::BodyPeer(attributed)
        | zakura_header_chain::Attribution::AuxPeer(attributed) => Some(attributed),
        zakura_header_chain::Attribution::None => None,
    };
    if attributed_source == Some(source) {
        VctRepairRetry::supplier(source, terminal)
    } else {
        VctRepairRetry::local(source, HeaderRequestTerminal::LocalError)
    }
}

/// Stall output that [`ReactorState::refresh_repair_stall`] found due.
#[derive(Clone, Debug)]
pub(super) struct RepairStallDue {
    pub(super) task: RepairRequirement,
    pub(super) record: VctRepairStall,
    /// Emit one stall trace row.
    pub(super) trace: bool,
    /// Report the stall at error level.
    pub(super) report: bool,
    pub(super) stalled_for: Duration,
    pub(super) best_peer_tip: Option<(block::Height, block::Hash)>,
}

/// The local operation's state at one deadline check.
#[derive(Clone, Debug)]
pub(super) enum LocalOperationExpiry {
    /// No local operation runs.
    Idle,
    /// The operation runs within its deadline.
    Running,
    /// The operation was already reported as fatal.
    AlreadyFatal,
    /// The operation passed its deadline at this check.
    Expired(VctLocalOperation),
}

#[derive(Clone, Debug)]
pub(super) struct VctLocalOperation {
    pub(super) phase: HeaderTargetPhase,
    pub(super) owner: zakura_header_chain::HeaderSyncWorkOwner,
    pub(super) repair_generation: u64,
    pub(super) target: zakura_header_chain::Frontier,
    pub(super) started_at: Instant,
    pub(super) deadline: Instant,
    pub(super) fatal_sent: bool,
}

impl VctLocalOperation {
    pub(super) fn from_action(action: &HeaderPortOperation, now: Instant) -> Option<Self> {
        let (phase, purpose, owner) = match action {
            HeaderPortOperation::PrepareHeaderTarget { purpose, owner, .. } => {
                (HeaderTargetPhase::Preparing, purpose, *owner)
            }
            HeaderPortOperation::ApplyHeaderTarget { purpose, owner, .. } => {
                (HeaderTargetPhase::Applying, purpose, *owner)
            }
            _ => return None,
        };
        let HeaderTargetPurpose::SelectedAuxiliaryRepair {
            selected_target,
            repair_generation,
            ..
        } = purpose
        else {
            return None;
        };
        Some(Self {
            phase,
            owner,
            repair_generation: *repair_generation,
            target: *selected_target,
            started_at: now,
            deadline: now + VCT_LOCAL_OPERATION_FATAL_AFTER,
            fatal_sent: false,
        })
    }

    pub(super) fn phase_label(&self) -> &'static str {
        match self.phase {
            HeaderTargetPhase::Preparing => "prepare",
            HeaderTargetPhase::Applying => "apply",
            HeaderTargetPhase::Receiving => {
                unreachable!("only local VCT operation phases have hard deadlines")
            }
        }
    }
}
