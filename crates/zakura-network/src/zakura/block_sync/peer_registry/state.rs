//! Logical owners of the block-sync peer registry.
//!
//! [`PeerTable`] owns the per-peer summaries. Body-retry avoidance is one logical
//! owner split across two locks: [`SupplierRetryState`] holds supplier-specific
//! deferrals and their authority, and [`BodyRetryAlarms`] holds the persisted
//! all-supplier alarm. Operations that rewrite or read both take both, and the
//! caller holds both locks in lookup order (alarms, then suppliers).
//!
//! These owners read no clock, lock, atomic, or channel. Time enters as `now`.
//! [`PeerRegistry`](super::PeerRegistry) keeps the locks, the floor-ranking
//! notifier, the session parks, the generation counter, and the metrics. A
//! transition that can change another routine's floor preference returns `true`;
//! the registry then wakes floor deferrers after unlocking. Removed entries and
//! detached claims return to the registry, which drops them after unlocking.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use zakura_chain::block;
use zakura_header_chain::{BodyWorkAuthority, BodyWorkOwner, SourceId};
use zakura_rt::collections::HashMap;

use super::{
    super::{
        config::{
            clamp_advertised_blocks, clamp_advertised_inflight, clamp_advertised_response_bytes,
        },
        BlockSyncStatus, ServicePeerDirection, ZakuraPeerId,
    },
    DirectionStatusCounts, Entry, OutstandingClaim, OutstandingMeta, SlotDiagnostics, SlotSummary,
    MAX_SUPPLIER_RETRIES,
};
use crate::zakura::ZakuraConnId;

// ---- peer summaries ----

/// Per-peer facts keyed by peer identity. Each entry belongs to one routine
/// generation; generation-gated writers ignore a superseded routine.
#[derive(Debug, Default)]
pub(super) struct PeerTable {
    peers: HashMap<ZakuraPeerId, Entry>,
}

impl PeerTable {
    #[cfg(test)]
    pub(super) fn get(&self, peer: &ZakuraPeerId) -> Option<&Entry> {
        self.peers.get(peer)
    }

    #[cfg(test)]
    pub(super) fn get_mut(&mut self, peer: &ZakuraPeerId) -> Option<&mut Entry> {
        self.peers.get_mut(peer)
    }

    /// Suppliers whose published status can serve `height`.
    pub(super) fn eligible_sources(&self, height: block::Height) -> BTreeSet<SourceId> {
        self.peers
            .iter()
            .filter(|(_, entry)| {
                entry.received_status
                    && entry.servable_low <= height
                    && height <= entry.servable_high
            })
            .map(|(peer, _)| SourceId::from_digest(peer.digest()))
            .collect()
    }

    /// Whether `generation` on `conn_id` still owns the peer's entry. Only that
    /// routine may record a park.
    pub(super) fn owns_session(
        &self,
        peer: &ZakuraPeerId,
        conn_id: ZakuraConnId,
        generation: u64,
    ) -> bool {
        self.peers
            .get(peer)
            .is_some_and(|entry| entry.generation == generation && entry.conn_id == Some(conn_id))
    }

    /// Release the entry's connection ownership if `conn_id` holds it.
    pub(super) fn release_connection(&mut self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        if let Some(entry) = self.peers.get_mut(peer) {
            if entry.conn_id == Some(conn_id) {
                entry.conn_id = None;
            }
        }
    }

    /// Install a fresh entry for a new routine generation. Returns the replaced
    /// session's entry for the caller to drop after unlocking.
    pub(super) fn admit(&mut self, peer: &ZakuraPeerId, entry: Entry) -> Option<Entry> {
        self.peers.insert(peer.clone(), entry)
    }

    /// Mark `generation`'s session closed. A newer generation is left open.
    pub(super) fn close_session(&mut self, peer: &ZakuraPeerId, generation: u64) {
        if let Some(entry) = self
            .peers
            .get_mut(peer)
            .filter(|entry| entry.generation == generation)
        {
            entry.session_closed = true;
        }
    }

    /// Remove the peer's entry if its session is closed. Returns the removed entry.
    pub(super) fn remove_closed_session(&mut self, peer: &ZakuraPeerId) -> Option<Entry> {
        if self
            .peers
            .get(peer)
            .is_some_and(|entry| entry.session_closed)
        {
            self.peers.remove(peer)
        } else {
            None
        }
    }

    #[cfg(test)]
    pub(super) fn remove(&mut self, peer: &ZakuraPeerId) -> Option<Entry> {
        self.peers.remove(peer)
    }

    /// Apply a status from the owning generation. Returns whether the peer's
    /// floor eligibility changed.
    #[must_use = "an eligibility change must wake floor deferrers"]
    pub(super) fn upsert_status(
        &mut self,
        peer: &ZakuraPeerId,
        generation: u64,
        status: BlockSyncStatus,
    ) -> bool {
        let Some(entry) = self.peers.get_mut(peer) else {
            return false;
        };
        if entry.generation != generation {
            return false;
        }
        let eligibility_changed = !entry.received_status
            || entry.servable_low != status.servable_low
            || entry.servable_high != status.servable_high;
        entry.servable_low = status.servable_low;
        entry.servable_high = status.servable_high;
        entry.max_blocks_per_response = clamp_advertised_blocks(status.max_blocks_per_response);
        entry.max_inflight_requests = clamp_advertised_inflight(status.max_inflight_requests);
        entry.max_response_bytes = clamp_advertised_response_bytes(status.max_response_bytes);
        entry.received_status = true;
        eligibility_changed
    }

    /// Publish claims from the owning generation. A claim replaces any stale
    /// claim at the same height.
    pub(super) fn insert_outstanding(
        &mut self,
        peer: &ZakuraPeerId,
        generation: u64,
        claims: impl IntoIterator<Item = (block::Height, OutstandingMeta)>,
    ) {
        if let Some(entry) = self.peers.get_mut(peer) {
            if entry.generation == generation {
                entry.outstanding.extend(claims);
            }
        }
    }

    /// Remove `owner`'s claims at `heights` for the owning generation. Another
    /// owner's claim stays published.
    pub(super) fn remove_outstanding_for_owner(
        &mut self,
        peer: &ZakuraPeerId,
        generation: u64,
        owner: BodyWorkOwner,
        heights: impl IntoIterator<Item = block::Height>,
    ) {
        let Some(entry) = self.peers.get_mut(peer) else {
            return;
        };
        if entry.generation != generation {
            return;
        }
        for height in heights {
            if entry.outstanding.get(&height).map(|meta| meta.owner) == Some(owner) {
                entry.outstanding.remove(&height);
            }
        }
    }

    /// Detach the owning generation's claims for the caller to drop after unlocking.
    pub(super) fn take_outstanding(
        &mut self,
        peer: &ZakuraPeerId,
        generation: u64,
    ) -> Option<BTreeMap<block::Height, OutstandingMeta>> {
        self.peers
            .get_mut(peer)
            .filter(|entry| entry.generation == generation)
            .map(|entry| std::mem::take(&mut entry.outstanding))
    }

    /// Publish the owning generation's slot diagnostics. Returns whether the
    /// change can alter floor preference.
    #[must_use = "a ranking change must wake floor deferrers"]
    pub(super) fn publish_slots(
        &mut self,
        peer: &ZakuraPeerId,
        generation: u64,
        slots: SlotDiagnostics,
    ) -> bool {
        let Some(entry) = self.peers.get_mut(peer) else {
            return false;
        };
        if entry.generation != generation {
            return false;
        }
        let ranking_changed = entry.slots.bbr_rtprop_ms != slots.bbr_rtprop_ms
            || (entry.slots.available_slots > 0) != (slots.available_slots > 0);
        entry.slots = slots;
        ranking_changed
    }

    /// Remove a published claim only when its exact owner still matches.
    pub(super) fn clear_outstanding_height_for_owner(
        &mut self,
        peer: &ZakuraPeerId,
        height: block::Height,
        owner: BodyWorkOwner,
    ) -> bool {
        let Some(entry) = self.peers.get_mut(peer) else {
            return false;
        };
        if entry.outstanding.get(&height).map(|meta| meta.owner) != Some(owner) {
            return false;
        }
        entry.outstanding.remove(&height);
        true
    }

    /// Hard-exclude this peer from re-taking `height` until `until`.
    pub(super) fn avoid_floor_height_until(
        &mut self,
        peer: &ZakuraPeerId,
        height: block::Height,
        until: Instant,
    ) {
        if let Some(entry) = self.peers.get_mut(peer) {
            entry.floor_watchdog_avoid.insert(height, until);
        }
    }

    /// Whether the floor watchdog still excludes this peer from `height`.
    /// Purges the peer's expired exclusions.
    pub(super) fn is_floor_height_avoided(
        &mut self,
        peer: &ZakuraPeerId,
        height: block::Height,
        now: Instant,
    ) -> bool {
        let Some(entry) = self.peers.get_mut(peer) else {
            return false;
        };
        entry.floor_watchdog_avoid.retain(|_, until| *until > now);
        entry
            .floor_watchdog_avoid
            .get(&height)
            .is_some_and(|until| *until > now)
    }

    /// The peer's next floor-watchdog exclusion expiry. Purges expired exclusions.
    pub(super) fn next_floor_avoid_deadline(
        &mut self,
        peer: &ZakuraPeerId,
        now: Instant,
    ) -> Option<Instant> {
        let entry = self.peers.get_mut(peer)?;
        entry.floor_watchdog_avoid.retain(|_, until| *until > now);
        entry.floor_watchdog_avoid.values().min().copied()
    }

    // ---- immutable observations ----

    pub(super) fn slot_summary(&self) -> SlotSummary {
        let mut summary = SlotSummary::default();
        for entry in self.peers.values() {
            summary.outstanding_requests = summary
                .outstanding_requests
                .saturating_add(entry.slots.outstanding_requests);
            if !entry.received_status {
                continue;
            }
            summary.capacity = summary.capacity.saturating_add(entry.slots.hard_capacity);
            summary.effective_window = summary
                .effective_window
                .saturating_add(entry.slots.effective_window);
            summary.available = summary
                .available
                .saturating_add(entry.slots.available_slots);
            if entry.slots.available_slots == 0 {
                summary.saturated_peers = summary.saturated_peers.saturating_add(1);
            }
        }
        summary
    }

    pub(super) fn has_outstanding_request(&self, height: block::Height, hash: block::Hash) -> bool {
        self.peers.values().any(|entry| {
            entry
                .outstanding
                .get(&height)
                .is_some_and(|meta| meta.hash == hash)
        })
    }

    pub(super) fn peer_has_outstanding_height(
        &self,
        peer: &ZakuraPeerId,
        height: block::Height,
    ) -> bool {
        self.peers
            .get(peer)
            .is_some_and(|entry| entry.outstanding.contains_key(&height))
    }

    pub(super) fn total_unreceived(&self) -> usize {
        self.peers
            .values()
            .map(|entry| entry.outstanding.len())
            .sum()
    }

    pub(super) fn any_outstanding_at_or_above(&self, at_or_above: block::Height) -> bool {
        self.peers.values().any(|entry| {
            entry
                .outstanding
                .keys()
                .any(|height| *height >= at_or_above)
        })
    }

    pub(super) fn any_outstanding_conflicts_at(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> bool {
        self.peers.values().any(|entry| {
            entry
                .outstanding
                .get(&height)
                .is_some_and(|expected| expected.hash != hash)
        })
    }

    pub(super) fn has_received_status(&self, peer: &ZakuraPeerId) -> bool {
        self.peers
            .get(peer)
            .is_some_and(|entry| entry.received_status)
    }

    pub(super) fn peers_with_status(&self) -> usize {
        self.peers
            .values()
            .filter(|entry| entry.received_status)
            .count()
    }

    pub(super) fn candidate_snapshot(
        &self,
    ) -> Vec<(ZakuraPeerId, bool, block::Height, block::Height)> {
        self.peers
            .iter()
            .map(|(peer, entry)| {
                (
                    peer.clone(),
                    entry.received_status,
                    entry.servable_low,
                    entry.servable_high,
                )
            })
            .collect()
    }

    pub(super) fn direction_status_counts(&self) -> DirectionStatusCounts {
        let mut counts = DirectionStatusCounts::default();
        for entry in self.peers.values() {
            match entry.direction {
                ServicePeerDirection::Inbound => {
                    counts.inbound += 1;
                    if entry.received_status {
                        counts.inbound_with_status += 1;
                    }
                }
                ServicePeerDirection::Outbound => {
                    counts.outbound += 1;
                    if entry.received_status {
                        counts.outbound_with_status += 1;
                    }
                }
            }
        }
        counts
    }

    pub(super) fn floor_gap_servable(&self, height: block::Height) -> (usize, usize) {
        let mut servable = 0usize;
        let mut outstanding = 0usize;
        for entry in self.peers.values() {
            if entry.received_status
                && entry.servable_low <= height
                && height <= entry.servable_high
            {
                servable = servable.saturating_add(1);
            }
            if entry.outstanding.contains_key(&height) {
                outstanding = outstanding.saturating_add(1);
            }
        }
        (servable, outstanding)
    }

    pub(super) fn earliest_outstanding_deadline_at(
        &self,
        height: block::Height,
    ) -> Option<Instant> {
        self.peers
            .values()
            .filter_map(|entry| entry.outstanding.get(&height).map(|meta| meta.deadline))
            .min()
    }

    /// See [`PeerRegistry::floor_has_preferred_unsaturated_server`](super::PeerRegistry::floor_has_preferred_unsaturated_server).
    pub(super) fn floor_has_preferred_unsaturated_server(
        &self,
        height: block::Height,
        self_peer: &ZakuraPeerId,
        self_rtprop_ms: Option<u64>,
        allow_equal_score: bool,
    ) -> bool {
        // Rank registered peers from the same published snapshot. Mixing a fresh
        // local expiry with other peers' cached scores can make every peer defer.
        let self_score = self
            .peers
            .get(self_peer)
            .map(|entry| entry.slots.bbr_rtprop_ms)
            .unwrap_or(self_rtprop_ms)
            .unwrap_or(u64::MAX);
        self.peers.iter().any(|(peer, entry)| {
            if peer == self_peer || !entry.can_serve_with_room(height) {
                return false;
            }
            let other_score = entry.slots.bbr_rtprop_ms.unwrap_or(u64::MAX);
            if allow_equal_score {
                other_score <= self_score
            } else {
                other_score < self_score
            }
        })
    }

    pub(super) fn outstanding_claims_at(&self, height: block::Height) -> Vec<OutstandingClaim> {
        self.peers
            .iter()
            .filter_map(|(peer, entry)| {
                entry.outstanding.get(&height).map(|meta| OutstandingClaim {
                    peer: peer.clone(),
                    height,
                    meta: *meta,
                })
            })
            .collect()
    }
}

// ---- body-retry avoidance ----

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum BodyRetryReason {
    Unavailable,
    Incompatible,
}

type SupplierRetryKey = (SourceId, BodyRetryKey, BodyRetryReason);
type SupplierRetries = HashMap<SupplierRetryKey, Instant>;

/// Rekeying can merge snapshots of one cooldown. Keep its latest deadline.
fn collect_supplier_retries(
    entries: impl Iterator<Item = (SupplierRetryKey, Instant)>,
) -> SupplierRetries {
    entries.fold(SupplierRetries::new(), |mut merged, (key, deadline)| {
        merged
            .entry(key)
            .and_modify(|until| *until = (*until).max(deadline))
            .or_insert(deadline);
        merged
    })
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
struct BodyRetryKey {
    header_generation: zakura_header_chain::HeaderGeneration,
    branch: zakura_header_chain::BranchId,
    body_work_epoch: zakura_header_chain::BodyWorkEpoch,
    hash: block::Hash,
}

impl BodyRetryKey {
    fn new(scope: BodyWorkAuthority, hash: block::Hash) -> Self {
        Self {
            header_generation: scope.header_generation,
            branch: scope.branch,
            body_work_epoch: scope.body_work_epoch,
            hash,
        }
    }
}

/// Supplier-specific retry deferrals and the authority they are keyed under.
/// Authority and deadlines share a lock so stale routine snapshots cannot undo
/// a refresh.
#[derive(Debug, Default)]
pub(super) struct SupplierRetryState {
    current: Option<BodyWorkAuthority>,
    entries: SupplierRetries,
}

impl SupplierRetryState {
    /// Install the exact set of suppliers whose unavailability defers one body.
    /// Deferrals for departed suppliers are removed. Returns the number of
    /// deferrals skipped at capacity.
    pub(super) fn defer_unavailable(
        &mut self,
        sources: BTreeSet<SourceId>,
        scope: BodyWorkAuthority,
        hash: block::Hash,
        until: Instant,
    ) -> u64 {
        let key = BodyRetryKey::new(scope, hash);
        self.entries.retain(|(source, candidate, reason), _| {
            *reason != BodyRetryReason::Unavailable || *candidate != key || sources.contains(source)
        });
        let mut skipped = 0;
        for source in sources {
            let entry = (source, key, BodyRetryReason::Unavailable);
            if self.entries.contains_key(&entry) || self.entries.len() < MAX_SUPPLIER_RETRIES {
                self.entries.insert(entry, until);
            } else {
                skipped += 1;
            }
        }
        skipped
    }

    /// Extend `source`'s compatibility cooldowns. Expired deferrals are purged
    /// first. Work from an older epoch than the current authority is ignored.
    /// Returns the number of cooldowns skipped at capacity.
    pub(super) fn defer_incompatible(
        &mut self,
        source: SourceId,
        bodies: Vec<(BodyWorkAuthority, block::Hash)>,
        until: Instant,
        now: Instant,
    ) -> u64 {
        self.entries.retain(|_, deadline| *deadline > now);
        let mut skipped = 0;
        for (scope, hash) in bodies {
            let scope = match self.current {
                Some(current) if current.body_work_epoch == scope.body_work_epoch => current,
                Some(current) if current.body_work_epoch > scope.body_work_epoch => continue,
                // Fresh work can reach a routine before the sequencer handles its reset.
                _ => scope,
            };
            let key = (
                source,
                BodyRetryKey::new(scope, hash),
                BodyRetryReason::Incompatible,
            );
            if let Some(deadline) = self.entries.get_mut(&key) {
                *deadline = (*deadline).max(until);
            } else if self.entries.len() < MAX_SUPPLIER_RETRIES {
                self.entries.insert(key, until);
            } else {
                // The cooldown is advisory. Skipping it only allows an earlier retry.
                skipped += 1;
            }
        }
        skipped
    }

    /// Remove every supplier's deferral of one exact body.
    pub(super) fn clear(&mut self, scope: BodyWorkAuthority, hash: block::Hash) {
        let key = BodyRetryKey::new(scope, hash);
        self.entries
            .retain(|(_, candidate, _), _| *candidate != key);
    }

    /// The supplier's next deferral expiry. Purges every expired deferral.
    pub(super) fn next_deadline(&mut self, source: SourceId, now: Instant) -> Option<Instant> {
        self.entries.retain(|_, until| *until > now);
        self.entries
            .iter()
            .filter_map(|((candidate, _, _), until)| (*candidate == source).then_some(*until))
            .min()
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }
}

/// The persisted all-supplier retry alarm. It holds at most one body.
#[derive(Debug, Default)]
pub(super) struct BodyRetryAlarms {
    alarms: HashMap<BodyRetryKey, Instant>,
}

impl BodyRetryAlarms {
    /// Replace the alarm.
    pub(super) fn set(&mut self, alarm: Option<(BodyWorkAuthority, block::Hash, Instant)>) {
        self.alarms.clear();
        if let Some((scope, hash, until)) = alarm {
            self.alarms.insert(BodyRetryKey::new(scope, hash), until);
        }
    }

    /// Remove the alarm for one exact body.
    pub(super) fn clear(&mut self, scope: BodyWorkAuthority, hash: block::Hash) {
        self.alarms.remove(&BodyRetryKey::new(scope, hash));
    }

    /// The alarm's expiry. Purges an expired alarm.
    pub(super) fn next_deadline(&mut self, now: Instant) -> Option<Instant> {
        self.alarms.retain(|_, until| *until > now);
        self.alarms.values().copied().min()
    }
}

/// Retire old work while preserving alarms and incompatibility reports from
/// newer work. `None` clears both maps.
pub(super) fn retain_body_retry_scope(
    alarms: &mut BodyRetryAlarms,
    suppliers: &mut SupplierRetryState,
    current: Option<BodyWorkAuthority>,
) {
    suppliers.current = current;
    suppliers.entries = collect_supplier_retries(
        std::mem::take(&mut suppliers.entries)
            .into_iter()
            .filter_map(|((source, mut key, reason), deadline)| {
                let scope = current?;
                if reason == BodyRetryReason::Incompatible {
                    if key.body_work_epoch < scope.body_work_epoch {
                        return None;
                    }
                    if key.body_work_epoch == scope.body_work_epoch {
                        key = BodyRetryKey::new(scope, key.hash);
                    }
                    // A newer epoch may already have reported an incompatibility.
                } else if key != BodyRetryKey::new(scope, key.hash) {
                    return None;
                }
                Some(((source, key, reason), deadline))
            }),
    );
    alarms.alarms = std::mem::take(&mut alarms.alarms)
        .into_iter()
        .filter_map(|(mut key, deadline)| {
            let scope = current?;
            if key.body_work_epoch < scope.body_work_epoch {
                return None;
            }
            if key.body_work_epoch == scope.body_work_epoch {
                key = BodyRetryKey::new(scope, key.hash);
            }
            // The reactor can publish a newer alarm before this reset is processed.
            Some((key, deadline))
        })
        .collect();
}

/// Carry suppression deadlines through a compatible authority refresh.
pub(super) fn refresh_body_retry_scope(
    alarms: &mut BodyRetryAlarms,
    suppliers: &mut SupplierRetryState,
    current: BodyWorkAuthority,
) {
    suppliers.current = Some(current);
    alarms.alarms = std::mem::take(&mut alarms.alarms)
        .into_iter()
        .map(|(mut key, deadline)| {
            if key.body_work_epoch == current.body_work_epoch {
                key = BodyRetryKey::new(current, key.hash);
            }
            (key, deadline)
        })
        .collect();
    suppliers.entries =
        collect_supplier_retries(std::mem::take(&mut suppliers.entries).into_iter().map(
            |((source, mut key, reason), deadline)| {
                // Compatible refreshes cannot adopt a cooldown from a future epoch.
                if reason != BodyRetryReason::Incompatible
                    || key.body_work_epoch == current.body_work_epoch
                {
                    key = BodyRetryKey::new(current, key.hash);
                }
                ((source, key, reason), deadline)
            },
        ));
}

/// Whether `source` must not be asked for this body at `now`. Matches supplier
/// cooldowns under both the caller's snapshot and the current authority while
/// work snapshots refresh.
pub(super) fn is_body_retry_avoided(
    alarms: &BodyRetryAlarms,
    suppliers: &SupplierRetryState,
    source: SourceId,
    scope: BodyWorkAuthority,
    hash: block::Hash,
    now: Instant,
) -> bool {
    // Check each deadline instead of sweeping: this runs once per scheduled
    // body, and insertion and `next_deadline` purge expired entries.
    let current = suppliers
        .current
        .filter(|current| current.body_work_epoch == scope.body_work_epoch)
        .unwrap_or(scope);
    let keys = [
        BodyRetryKey::new(current, hash),
        BodyRetryKey::new(scope, hash),
    ];
    // There is at most one published alarm. Its exact body stays suppressed
    // across compatible snapshots, including before either reader has refreshed.
    alarms.alarms.iter().any(|(key, until)| {
        *until > now && key.hash == hash && key.body_work_epoch == scope.body_work_epoch
    }) || keys.into_iter().any(|key| {
        [BodyRetryReason::Unavailable, BodyRetryReason::Incompatible]
            .into_iter()
            .any(|reason| {
                suppliers
                    .entries
                    .get(&(source, key, reason))
                    .is_some_and(|until| *until > now)
            })
    })
}
