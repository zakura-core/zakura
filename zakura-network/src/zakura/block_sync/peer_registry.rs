//! Shared per-peer fact table for Zakura block sync (per-peer routines).
//!
//! Per-peer routines move all per-peer *download* state and the take-work decision off the
//! reactor's single loop into a spawned [`PeerRoutine`](super::peer_routine) per
//! connected peer. The [`PeerRegistry`] is the small shared table the reactor
//! still needs for *global* decisions — admission counting, the producer's
//! `!has_outstanding_request` filter, the low-water `total_unreceived` gate, and
//! candidate publication — plus the per-peer servable range / caps the routine
//! reads back when it runs its want-work loop.
//!
//! Field ownership is disjoint so the brief `std::sync::Mutex` is never a
//! contention point and is **never held across `.await`** (the anti-block rule).
//! After inbound flow is inverted the **routine** is authoritative for its own
//! per-peer facts and writes them all (generation-gated): servable/caps/
//! `received_status` (when it decodes a `Status` frame in its own task),
//! `outstanding` (on issue/finish/timeout/disconnect — per *request*, never per
//! *body*), slot diagnostics, and download-side misbehavior. The **reactor** owns
//! entry insert/remove (admission/teardown), serving-side misbehavior, and
//! floor-watchdog hard excludes. Misbehavior is record-only: it is observed and
//! traced but never drives a disconnect, so the registry keeps no per-peer
//! misbehavior state.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Mutex as StdMutex,
    time::Instant,
};

use zakura_chain::block;

use super::{
    config::{clamp_advertised_blocks, clamp_advertised_inflight, clamp_advertised_response_bytes},
    state::EFFECTIVE_BS_OUTBOUND_INFLIGHT_PER_PEER,
    BlockSyncStatus, ServicePeerDirection, ZakuraPeerId,
};
use crate::zakura::ZakuraConnId;

/// Per-peer facts the reactor needs globally and the routine reads back.
#[derive(Clone, Debug)]
pub(super) struct Entry {
    pub(super) direction: ServicePeerDirection,
    pub(super) servable_low: block::Height,
    pub(super) servable_high: block::Height,
    pub(super) received_status: bool,
    pub(super) max_blocks_per_response: u32,
    pub(super) max_inflight_requests: u32,
    pub(super) max_response_bytes: u32,
    /// The height→hash set of this peer's *unreceived* in-flight request heights.
    /// Per-*request* granularity (each outstanding `BlockRangeRequest` contributes
    /// its still-unreceived expected heights), never per-body. This is the Sequencer task
    /// producer filter's `!has_outstanding_request` home, now routine-owned and
    /// independent of `work.in_flight`, so it structurally closes the
    /// reject-rollback window.
    pub(super) outstanding: BTreeMap<block::Height, OutstandingMeta>,
    /// Routine-published slot and BBR diagnostics. The reactor summarizes this for
    /// the periodic `BLOCK_SYNC_STATE` row, and peer routines read it for cross-peer
    /// floor-bias decisions. Updated whenever the routine issues/finishes/times out
    /// a request.
    pub(super) slots: SlotDiagnostics,
    /// Heights this peer may not re-take after a floor-watchdog cancellation.
    pub(super) floor_watchdog_avoid: BTreeMap<block::Height, Instant>,
    /// Monotonic generation bumped each time a routine is (re)spawned for this
    /// peer. A cancelled routine's async `Drop` only clears outstanding when the
    /// generation still matches, so an old Drop racing a reset respawn cannot wipe
    /// the live routine's published outstanding.
    pub(super) generation: u64,
}

impl Entry {
    fn new(
        direction: ServicePeerDirection,
        config: &super::ZakuraBlockSyncConfig,
        generation: u64,
    ) -> Self {
        Self {
            direction,
            servable_low: block::Height::MIN,
            servable_high: block::Height::MIN,
            received_status: false,
            max_blocks_per_response: config.advertised_max_blocks_per_response(),
            max_inflight_requests: config.advertised_max_inflight_requests(),
            max_response_bytes: config.advertised_max_response_bytes(),
            outstanding: BTreeMap::new(),
            slots: SlotDiagnostics::default(),
            floor_watchdog_avoid: BTreeMap::new(),
            generation,
        }
    }
}

/// Per-peer download window diagnostics published by the routine for trace
/// summaries and cross-peer floor-bias decisions.
#[derive(Copy, Clone, Debug, Default)]
pub(super) struct SlotDiagnostics {
    pub(super) hard_capacity: usize,
    pub(super) effective_window: usize,
    pub(super) available_slots: usize,
    pub(super) outstanding_requests: usize,
    pub(super) bbr_rtprop_ms: Option<u64>,
}

/// Published metadata for one unreceived outstanding height.
#[derive(Copy, Clone, Debug)]
pub(super) struct OutstandingMeta {
    pub(super) hash: block::Hash,
    pub(super) estimated_bytes: u64,
    pub(super) queued_at: Instant,
    pub(super) deadline: Instant,
}

/// Reactor-visible claim that can be force-cancelled by the floor watchdog.
#[derive(Clone, Debug)]
pub(super) struct OutstandingClaim {
    pub(super) peer: ZakuraPeerId,
    pub(super) height: block::Height,
    pub(super) meta: OutstandingMeta,
}

/// A no-progress park recorded by the routine that made the decision.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct SessionPark {
    /// The connection whose session was parked. An expired park for this same
    /// connection remains gated on body work until it is re-admitted.
    conn_id: Option<ZakuraConnId>,
    /// Refuse block-sync admission for this peer until this deadline.
    deadline: Instant,
}

/// The shared per-peer fact table. `Arc`-wrapped at the construction site so the
/// reactor and every routine share one table.
#[derive(Debug)]
pub(super) struct PeerRegistry {
    peers: StdMutex<HashMap<ZakuraPeerId, Entry>>,
    session_parks: StdMutex<HashMap<ZakuraPeerId, SessionPark>>,
    /// Source of monotonically-increasing routine generations.
    next_generation: std::sync::atomic::AtomicU64,
}

impl Default for PeerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerRegistry {
    pub(super) fn new() -> Self {
        Self {
            peers: StdMutex::new(HashMap::new()),
            session_parks: StdMutex::new(HashMap::new()),
            next_generation: std::sync::atomic::AtomicU64::new(1),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ZakuraPeerId, Entry>> {
        self.peers
            .lock()
            .expect("peer registry mutex is never poisoned")
    }

    fn lock_session_parks(&self) -> std::sync::MutexGuard<'_, HashMap<ZakuraPeerId, SessionPark>> {
        self.session_parks
            .lock()
            .expect("peer registry session-park mutex is never poisoned")
    }

    /// Record the connection-local session park at the no-progress decision site.
    /// A superseded routine cannot park the replacement generation.
    pub(super) fn park_session(
        &self,
        peer: &ZakuraPeerId,
        conn_id: ZakuraConnId,
        generation: u64,
        deadline: Instant,
    ) -> bool {
        let peers = self.lock();
        if peers
            .get(peer)
            .is_none_or(|entry| entry.generation != generation)
        {
            return false;
        }
        self.lock_session_parks().insert(
            peer.clone(),
            SessionPark {
                conn_id: Some(conn_id),
                deadline,
            },
        );
        true
    }

    /// Refuse this peer at block-sync admission until `deadline` without associating
    /// the park with a live connection.
    #[cfg(test)]
    pub(super) fn park_peer_until(&self, peer: &ZakuraPeerId, deadline: Instant) {
        self.lock_session_parks().insert(
            peer.clone(),
            SessionPark {
                conn_id: None,
                deadline,
            },
        );
    }

    #[cfg(test)]
    pub(super) fn park_session_for_test(
        &self,
        peer: &ZakuraPeerId,
        conn_id: ZakuraConnId,
        deadline: Instant,
    ) {
        self.lock_session_parks().insert(
            peer.clone(),
            SessionPark {
                conn_id: Some(conn_id),
                deadline,
            },
        );
    }

    /// Return this peer's active local park deadline.
    pub(super) fn peer_park_deadline(&self, peer: &ZakuraPeerId, now: Instant) -> Option<Instant> {
        let mut session_parks = self.lock_session_parks();
        // An expired connection-associated park still carries the same-connection
        // body-work gate. Only expired parks with no live connection can be collected here.
        session_parks.retain(|_, park| park.deadline > now || park.conn_id.is_some());
        session_parks
            .get(peer)
            .filter(|park| park.deadline > now)
            .map(|park| park.deadline)
    }

    /// Whether the peer is still in its no-progress reconnect cooldown.
    pub(super) fn is_peer_parked(&self, peer: &ZakuraPeerId, now: Instant) -> bool {
        self.peer_park_deadline(peer, now).is_some()
    }

    /// Whether this connection owns an expired park and must wait for body work.
    pub(super) fn has_expired_session_park(
        &self,
        peer: &ZakuraPeerId,
        conn_id: ZakuraConnId,
        now: Instant,
    ) -> bool {
        self.lock_session_parks()
            .get(peer)
            .is_some_and(|park| park.conn_id == Some(conn_id) && park.deadline <= now)
    }

    /// Consume an expired park when admitting a stream. Returns whether this is
    /// the parked connection's one bounded re-admission. A different connection
    /// clears the stale association and is admitted normally.
    pub(super) fn take_session_park(
        &self,
        peer: &ZakuraPeerId,
        conn_id: ZakuraConnId,
        now: Instant,
    ) -> bool {
        let mut session_parks = self.lock_session_parks();
        let Some(park) = session_parks.get(peer).copied() else {
            return false;
        };
        if park.deadline > now {
            return false;
        }
        session_parks.remove(peer);
        park.conn_id == Some(conn_id)
    }

    /// Disassociate a closed connection from its park while preserving the
    /// peer-level cooldown. Expired records with no live connection are removed.
    pub(super) fn connection_closed(
        &self,
        peer: &ZakuraPeerId,
        conn_id: ZakuraConnId,
        now: Instant,
    ) {
        let mut session_parks = self.lock_session_parks();
        let Some(park) = session_parks.get_mut(peer) else {
            return;
        };
        if park.conn_id != Some(conn_id) {
            return;
        }
        if park.deadline <= now {
            session_parks.remove(peer);
        } else {
            park.conn_id = None;
        }
    }

    /// Admit (or re-admit) a peer and allocate a fresh routine generation.
    ///
    /// On a genuinely new peer this inserts a default entry; on a respawn (reset)
    /// the existing entry's servable/caps/`received_status` are preserved (the
    /// peer stays connected) but its outstanding set is cleared and its generation
    /// bumped, so the new routine owns the entry. Returns the generation the new
    /// routine must carry for its `Drop` guard.
    pub(super) fn admit(
        &self,
        peer: &ZakuraPeerId,
        direction: ServicePeerDirection,
        config: &super::ZakuraBlockSyncConfig,
    ) -> u64 {
        let generation = self
            .next_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut peers = self.lock();
        peers
            .entry(peer.clone())
            .and_modify(|entry| {
                entry.direction = direction;
                entry.outstanding.clear();
                entry.floor_watchdog_avoid.clear();
                entry.generation = generation;
            })
            .or_insert_with(|| Entry::new(direction, config, generation));
        generation
    }

    /// Remove a peer's entry entirely (disconnect/teardown/admission-reject).
    pub(super) fn remove(&self, peer: &ZakuraPeerId) {
        self.lock().remove(peer);
    }

    /// Publish a freshly-applied `Status` (routine-side, inverted inbound flow): grow
    /// servable range, clamp the advertised caps, and mark the peer as having sent
    /// a status. Generation-gated like the other routine writers so a superseded
    /// routine cannot clobber the live entry. No-op if the peer is gone.
    pub(super) fn upsert_status(
        &self,
        peer: &ZakuraPeerId,
        generation: u64,
        status: BlockSyncStatus,
    ) {
        let mut peers = self.lock();
        let Some(entry) = peers.get_mut(peer) else {
            return;
        };
        if entry.generation != generation {
            return;
        }
        entry.servable_low = status.servable_low;
        entry.servable_high = status.servable_high;
        entry.max_blocks_per_response = clamp_advertised_blocks(status.max_blocks_per_response);
        entry.max_inflight_requests = clamp_advertised_inflight(status.max_inflight_requests);
        entry.max_response_bytes = clamp_advertised_response_bytes(status.max_response_bytes);
        entry.received_status = true;
    }

    /// Replace the peer's outstanding height→hash set (routine-owned), but only if
    /// the routine's `generation` still owns the entry. A write from a routine
    /// that has been superseded by a respawn is dropped.
    pub(super) fn set_outstanding(
        &self,
        peer: &ZakuraPeerId,
        generation: u64,
        outstanding: BTreeMap<block::Height, OutstandingMeta>,
    ) {
        let mut peers = self.lock();
        if let Some(entry) = peers.get_mut(peer) {
            if entry.generation == generation {
                entry.outstanding = outstanding;
            }
        }
    }

    /// Clear the peer's outstanding set (it has no live requests), generation-gated
    /// as in [`set_outstanding`](Self::set_outstanding).
    pub(super) fn clear_outstanding(&self, peer: &ZakuraPeerId, generation: u64) {
        let mut peers = self.lock();
        if let Some(entry) = peers.get_mut(peer) {
            if entry.generation == generation {
                entry.outstanding.clear();
            }
        }
    }

    /// Publish the routine's download-window diagnostics, generation-gated like the
    /// outstanding writers. These feed both trace summaries and floor-bias decisions.
    pub(super) fn publish_slots(
        &self,
        peer: &ZakuraPeerId,
        generation: u64,
        slots: SlotDiagnostics,
    ) {
        let mut peers = self.lock();
        if let Some(entry) = peers.get_mut(peer) {
            if entry.generation == generation {
                entry.slots = slots;
            }
        }
    }

    /// Aggregate the routines' slot diagnostics for the periodic trace row.
    pub(super) fn slot_summary(&self) -> SlotSummary {
        let peers = self.lock();
        let mut summary = SlotSummary::default();
        for entry in peers.values() {
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

    /// Whether any connected peer has an outstanding request for `height`
    /// expecting `hash` (the producer's `!has_outstanding_request` filter and the
    /// `ignore_unmatched_active` fallthrough).
    pub(super) fn has_outstanding_request(&self, height: block::Height, hash: block::Hash) -> bool {
        let peers = self.lock();
        peers.values().any(|entry| {
            entry
                .outstanding
                .get(&height)
                .is_some_and(|meta| meta.hash == hash)
        })
    }

    /// Whether any connected peer has an outstanding request covering `height`
    /// (regardless of hash). Used by the routine's terminator-dedup fallthrough
    /// (`ignore_unmatched_active_terminator_response`): a `BlocksDone` for a range
    /// another peer is actively requesting is dropped quietly, not scored.
    pub(super) fn has_outstanding_height(&self, height: block::Height) -> bool {
        let peers = self.lock();
        peers
            .values()
            .any(|entry| entry.outstanding.contains_key(&height))
    }

    /// Whether this exact peer still owns an outstanding claim for `height`.
    pub(super) fn peer_has_outstanding_height(
        &self,
        peer: &ZakuraPeerId,
        height: block::Height,
    ) -> bool {
        let peers = self.lock();
        peers
            .get(peer)
            .is_some_and(|entry| entry.outstanding.contains_key(&height))
    }

    /// Total unreceived in-flight heights summed across peers — *per request*,
    /// never per body (an `outstanding` entry is one requested height). Feeds the
    /// producer's low-water refill gate.
    pub(super) fn total_unreceived(&self) -> usize {
        let peers = self.lock();
        peers.values().map(|entry| entry.outstanding.len()).sum()
    }

    /// Whether any peer has an outstanding request reaching height `at_or_above`
    /// (the `peer_has_successor_after` half of the reset decision). Reads the
    /// registry's per-height outstanding set across peers.
    pub(super) fn any_outstanding_at_or_above(&self, at_or_above: block::Height) -> bool {
        let peers = self.lock();
        peers.values().any(|entry| {
            entry
                .outstanding
                .keys()
                .any(|height| *height >= at_or_above)
        })
    }

    /// Whether any peer has an outstanding request whose expected hash at `height`
    /// differs from `hash` (the peer-outstanding clause of
    /// `reset_tip_conflicts_with_local_work`).
    pub(super) fn any_outstanding_conflicts_at(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> bool {
        let peers = self.lock();
        peers.values().any(|entry| {
            entry
                .outstanding
                .get(&height)
                .is_some_and(|expected| expected.hash != hash)
        })
    }

    /// Whether the peer has sent a `Status` (the reactor's serving-admission and
    /// disconnect-trace read). The routine owns the rest of the serving caps
    /// locally now (inverted inbound flow); only `received_status` is read reactor-side.
    pub(super) fn has_received_status(&self, peer: &ZakuraPeerId) -> bool {
        let peers = self.lock();
        peers.get(peer).is_some_and(|entry| entry.received_status)
    }

    /// Count of peers that have sent a status (low-water refill + trace).
    pub(super) fn peers_with_status(&self) -> usize {
        let peers = self.lock();
        peers.values().filter(|entry| entry.received_status).count()
    }

    /// Candidate snapshot: node-id-servable hint per peer, used to publish the
    /// block-sync candidate set. Returns `(received_status, servable_low,
    /// servable_high)` per peer so the reactor can compute `can_serve_any`.
    pub(super) fn candidate_snapshot(
        &self,
    ) -> Vec<(ZakuraPeerId, bool, block::Height, block::Height)> {
        let peers = self.lock();
        peers
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

    /// Per-direction peer / with-status counts for the periodic trace tick.
    pub(super) fn direction_status_counts(&self) -> DirectionStatusCounts {
        let peers = self.lock();
        let mut counts = DirectionStatusCounts::default();
        for entry in peers.values() {
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

    /// Snapshot for the `floor_gap_diagnostics` trace: for a target `height`,
    /// how many peers are servable and how many of those have an outstanding
    /// request covering it.
    pub(super) fn floor_gap_servable(&self, height: block::Height) -> (usize, usize) {
        let peers = self.lock();
        let mut servable = 0usize;
        let mut outstanding = 0usize;
        for entry in peers.values() {
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

    /// The soonest deadline among all peer claims for one height, if any. Lets the
    /// reactor arm its floor watchdog to the exact expiry without allocating a
    /// claim snapshot on every loop iteration.
    pub(super) fn earliest_outstanding_deadline_at(
        &self,
        height: block::Height,
    ) -> Option<Instant> {
        let peers = self.lock();
        peers
            .values()
            .filter_map(|entry| entry.outstanding.get(&height).map(|meta| meta.deadline))
            .min()
    }

    /// Whether some peer other than `self_peer` is a preferred floor server for
    /// `height`: servable for it, holding a free normal (non-bypass) slot, and a
    /// better floor server by RTprop. "Better" is strictly lower RTprop, or — when
    /// `allow_equal_score` — equal-or-lower.
    ///
    /// The floor rides the fastest servable carrier. The normal take path passes
    /// `allow_equal_score = false`, so this peer defers the floor only to a strictly
    /// faster carrier; equal-RTprop carriers all stay eligible and the single-owner
    /// work queue assigns one of them. The floor-bypass path passes
    /// `allow_equal_score = true`, so a peer whose cwnd is saturated yields its scarce
    /// bypass slot to an equal-or-faster peer that can take the floor through normal
    /// capacity. Deadlock-free either way: the unique fastest unsaturated server is
    /// never preferred over (nothing beats it), and if every servable peer is
    /// saturated this returns false and the floor still moves. Unknown RTprop is
    /// treated as worst, so a measured peer is never deferred to an unmeasured one.
    pub(super) fn floor_has_preferred_unsaturated_server(
        &self,
        height: block::Height,
        self_peer: &ZakuraPeerId,
        self_rtprop_ms: Option<u64>,
        allow_equal_score: bool,
    ) -> bool {
        let self_score = self_rtprop_ms.unwrap_or(u64::MAX);
        let peers = self.lock();
        peers.iter().any(|(peer, entry)| {
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

    /// Snapshot all peer claims for one height.
    pub(super) fn outstanding_claims_at(&self, height: block::Height) -> Vec<OutstandingClaim> {
        let peers = self.lock();
        peers
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

    /// Remove a published outstanding claim for `height` from `peer`.
    pub(super) fn clear_outstanding_height(&self, peer: &ZakuraPeerId, height: block::Height) {
        let mut peers = self.lock();
        if let Some(entry) = peers.get_mut(peer) {
            entry.outstanding.remove(&height);
        }
    }

    /// Hard-exclude this peer from re-taking `height` until `until` after the
    /// floor watchdog force-cancels its stale claim.
    pub(super) fn avoid_floor_height_until(
        &self,
        peer: &ZakuraPeerId,
        height: block::Height,
        until: Instant,
    ) {
        let mut peers = self.lock();
        if let Some(entry) = peers.get_mut(peer) {
            entry.floor_watchdog_avoid.insert(height, until);
        }
    }

    /// Whether the floor watchdog still hard-excludes this peer from `height`.
    pub(super) fn is_floor_height_avoided(
        &self,
        peer: &ZakuraPeerId,
        height: block::Height,
        now: Instant,
    ) -> bool {
        let mut peers = self.lock();
        let Some(entry) = peers.get_mut(peer) else {
            return false;
        };
        entry.floor_watchdog_avoid.retain(|_, until| *until > now);
        entry
            .floor_watchdog_avoid
            .get(&height)
            .is_some_and(|until| *until > now)
    }

    /// The next floor-watchdog hard-exclude expiry for this peer, if any. The
    /// routine uses this to wake itself when a registry-owned avoid expires.
    pub(super) fn next_floor_avoid_deadline(
        &self,
        peer: &ZakuraPeerId,
        now: Instant,
    ) -> Option<Instant> {
        let mut peers = self.lock();
        let entry = peers.get_mut(peer)?;
        entry.floor_watchdog_avoid.retain(|_, until| *until > now);
        entry.floor_watchdog_avoid.values().min().copied()
    }
}

impl Entry {
    fn can_serve_with_room(&self, height: block::Height) -> bool {
        self.received_status
            && self.servable_low <= height
            && height <= self.servable_high
            && self.slots.available_slots > 0
    }
}

/// Aggregated slot diagnostics across peers for the periodic trace row.
#[derive(Copy, Clone, Debug, Default)]
pub(super) struct SlotSummary {
    pub(super) capacity: usize,
    pub(super) effective_window: usize,
    pub(super) available: usize,
    pub(super) saturated_peers: usize,
    pub(super) outstanding_requests: usize,
}

/// Per-direction peer counts for the periodic trace tick.
#[derive(Copy, Clone, Debug, Default)]
pub(super) struct DirectionStatusCounts {
    pub(super) inbound: usize,
    pub(super) outbound: usize,
    pub(super) inbound_with_status: usize,
    pub(super) outbound_with_status: usize,
}

/// Hard outbound concurrency ceiling for a peer with the given advertised
/// in-flight cap (the routine's slot bound).
pub(super) fn hard_outbound_capacity(max_inflight_requests: u32) -> usize {
    usize::try_from(max_inflight_requests)
        .expect("u32 max inflight requests fits in usize on supported targets")
        .min(EFFECTIVE_BS_OUTBOUND_INFLIGHT_PER_PEER)
}

#[cfg(test)]
mod floor_bias_tests {
    use super::*;

    fn peer(byte: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![byte; 32]).expect("32-byte test peer id is valid")
    }

    /// Register `peer` as servable for `[low, high]` with `available` free slots.
    fn register_with_rtprop(
        reg: &PeerRegistry,
        config: &super::super::ZakuraBlockSyncConfig,
        peer: &ZakuraPeerId,
        low: u32,
        high: u32,
        available: usize,
        bbr_rtprop_ms: Option<u64>,
    ) {
        let generation = reg.admit(peer, ServicePeerDirection::Outbound, config);
        reg.upsert_status(
            peer,
            generation,
            BlockSyncStatus {
                servable_low: block::Height(low),
                servable_high: block::Height(high),
                ..BlockSyncStatus::default()
            },
        );
        reg.publish_slots(
            peer,
            generation,
            SlotDiagnostics {
                available_slots: available,
                bbr_rtprop_ms,
                ..SlotDiagnostics::default()
            },
        );
    }

    fn register(
        reg: &PeerRegistry,
        config: &super::super::ZakuraBlockSyncConfig,
        peer: &ZakuraPeerId,
        low: u32,
        high: u32,
        available: usize,
    ) {
        register_with_rtprop(reg, config, peer, low, high, available, None);
    }

    #[test]
    fn bypass_defers_to_an_equal_or_faster_unsaturated_other_server() {
        let config = super::super::ZakuraBlockSyncConfig::default();
        let reg = PeerRegistry::new();
        let (a, b) = (peer(1), peer(2));
        // A is saturated; B serves the floor and has a free slot at an equal RTprop.
        register_with_rtprop(&reg, &config, &a, 0, 1000, 0, Some(50));
        register_with_rtprop(&reg, &config, &b, 0, 1000, 3, Some(50));
        // In the bypass region (include_equal) A defers — B can take the floor through
        // its normal capacity, so A keeps its scarce bypass slot…
        assert!(reg.floor_has_preferred_unsaturated_server(block::Height(100), &a, Some(50), true));
        // …but B itself has no other unsaturated server (A is saturated), so B bypasses.
        assert!(!reg.floor_has_preferred_unsaturated_server(
            block::Height(100),
            &b,
            Some(50),
            true
        ));
    }

    #[test]
    fn normal_path_defers_only_to_a_strictly_faster_server() {
        let config = super::super::ZakuraBlockSyncConfig::default();
        let reg = PeerRegistry::new();
        let (slow, fast) = (peer(1), peer(2));
        // Both unsaturated; the normal take path (include_equal = false).
        register_with_rtprop(&reg, &config, &slow, 0, 1000, 3, Some(120));
        register_with_rtprop(&reg, &config, &fast, 0, 1000, 3, Some(40));
        // The slow peer hands the floor up to the strictly-faster carrier…
        assert!(reg.floor_has_preferred_unsaturated_server(
            block::Height(100),
            &slow,
            Some(120),
            false
        ));
        // …and the fastest carrier never defers, so the floor always lands somewhere.
        assert!(!reg.floor_has_preferred_unsaturated_server(
            block::Height(100),
            &fast,
            Some(40),
            false
        ));
    }

    #[test]
    fn normal_path_keeps_equal_carriers_eligible() {
        let config = super::super::ZakuraBlockSyncConfig::default();
        let reg = PeerRegistry::new();
        let (a, b) = (peer(1), peer(2));
        // Two equal-RTprop unsaturated carriers: neither defers (strict <), so both stay
        // eligible and the single-owner work queue assigns the floor to one of them —
        // they never both defer and wedge the floor.
        register_with_rtprop(&reg, &config, &a, 0, 1000, 3, Some(50));
        register_with_rtprop(&reg, &config, &b, 0, 1000, 3, Some(50));
        assert!(!reg.floor_has_preferred_unsaturated_server(
            block::Height(100),
            &a,
            Some(50),
            false
        ));
        assert!(!reg.floor_has_preferred_unsaturated_server(
            block::Height(100),
            &b,
            Some(50),
            false
        ));
    }

    #[test]
    fn saturated_fast_peer_does_not_defer_to_slower_unsaturated_peer() {
        let config = super::super::ZakuraBlockSyncConfig::default();
        let reg = PeerRegistry::new();
        let (fast, slow) = (peer(1), peer(2));
        register_with_rtprop(&reg, &config, &fast, 0, 1000, 0, Some(40));
        register_with_rtprop(&reg, &config, &slow, 0, 1000, 3, Some(120));
        assert!(!reg.floor_has_preferred_unsaturated_server(
            block::Height(100),
            &fast,
            Some(40),
            true
        ));
    }

    #[test]
    fn bypasses_when_every_server_is_saturated() {
        let config = super::super::ZakuraBlockSyncConfig::default();
        let reg = PeerRegistry::new();
        let (a, b) = (peer(1), peer(2));
        register(&reg, &config, &a, 0, 1000, 0);
        register(&reg, &config, &b, 0, 1000, 0);
        assert!(!reg.floor_has_preferred_unsaturated_server(block::Height(100), &a, None, true));
    }

    #[test]
    fn ignores_an_unsaturated_peer_that_cannot_serve_the_floor() {
        let config = super::super::ZakuraBlockSyncConfig::default();
        let reg = PeerRegistry::new();
        let (a, b) = (peer(1), peer(2));
        register(&reg, &config, &a, 0, 1000, 0);
        // B has a free slot but only serves heights 500..=1000 — it cannot take a floor
        // request at height 100, so A must still bypass.
        register(&reg, &config, &b, 500, 1000, 3);
        assert!(!reg.floor_has_preferred_unsaturated_server(block::Height(100), &a, None, true));
    }

    #[test]
    fn floor_avoid_deadline_prunes_expired_entries_and_returns_next_wake() {
        let config = super::super::ZakuraBlockSyncConfig::default();
        let reg = PeerRegistry::new();
        let peer = peer(1);
        reg.admit(&peer, ServicePeerDirection::Outbound, &config);
        let now = Instant::now();

        reg.avoid_floor_height_until(
            &peer,
            block::Height(1),
            now - std::time::Duration::from_secs(1),
        );
        reg.avoid_floor_height_until(
            &peer,
            block::Height(2),
            now + std::time::Duration::from_secs(2),
        );
        reg.avoid_floor_height_until(
            &peer,
            block::Height(3),
            now + std::time::Duration::from_secs(1),
        );

        assert_eq!(
            reg.next_floor_avoid_deadline(&peer, now),
            Some(now + std::time::Duration::from_secs(1)),
        );
        assert!(!reg.is_floor_height_avoided(&peer, block::Height(1), now));
        assert!(reg.is_floor_height_avoided(&peer, block::Height(2), now));
    }

    #[test]
    fn parked_peer_expires_after_cooldown() {
        let reg = PeerRegistry::new();
        let peer = peer(1);
        let now = Instant::now();

        reg.park_peer_until(&peer, now + std::time::Duration::from_secs(1));

        assert!(reg.is_peer_parked(&peer, now));
        assert!(!reg.is_peer_parked(&peer, now + std::time::Duration::from_secs(2)));
    }

    #[test]
    fn expired_session_park_is_consumed_by_same_connection_readmission() {
        let reg = PeerRegistry::new();
        let peer = peer(2);
        let conn_id = 7;
        let now = Instant::now();
        let generation = reg.admit(
            &peer,
            ServicePeerDirection::Outbound,
            &super::super::ZakuraBlockSyncConfig::default(),
        );

        assert!(reg.park_session(
            &peer,
            conn_id,
            generation,
            now + std::time::Duration::from_secs(1),
        ));

        assert_eq!(
            reg.peer_park_deadline(&peer, now),
            Some(now + std::time::Duration::from_secs(1)),
        );
        assert!(reg.has_expired_session_park(
            &peer,
            conn_id,
            now + std::time::Duration::from_secs(2),
        ));
        assert!(reg.take_session_park(&peer, conn_id, now + std::time::Duration::from_secs(2)));
        assert!(!reg.has_expired_session_park(
            &peer,
            conn_id,
            now + std::time::Duration::from_secs(2),
        ));
    }

    #[test]
    fn connection_cleanup_preserves_cooldown_without_gating_a_fresh_connection() {
        let reg = PeerRegistry::new();
        let peer = peer(3);
        let old_conn_id = 7;
        let new_conn_id = 8;
        let now = Instant::now();
        let deadline = now + std::time::Duration::from_secs(1);
        let generation = reg.admit(
            &peer,
            ServicePeerDirection::Outbound,
            &super::super::ZakuraBlockSyncConfig::default(),
        );

        assert!(reg.park_session(&peer, old_conn_id, generation, deadline));
        reg.connection_closed(&peer, old_conn_id, now);

        assert_eq!(reg.peer_park_deadline(&peer, now), Some(deadline));
        assert!(!reg.has_expired_session_park(
            &peer,
            old_conn_id,
            now + std::time::Duration::from_secs(2),
        ));
        assert!(!reg.has_expired_session_park(
            &peer,
            new_conn_id,
            now + std::time::Duration::from_secs(2),
        ));
        assert!(!reg.is_peer_parked(&peer, now + std::time::Duration::from_secs(2),));
    }

    #[test]
    fn superseded_routine_cannot_park_the_replacement_generation() {
        let reg = PeerRegistry::new();
        let peer = peer(4);
        let config = super::super::ZakuraBlockSyncConfig::default();
        let old_generation = reg.admit(&peer, ServicePeerDirection::Outbound, &config);
        let _new_generation = reg.admit(&peer, ServicePeerDirection::Outbound, &config);
        let now = Instant::now();

        assert!(!reg.park_session(
            &peer,
            7,
            old_generation,
            now + std::time::Duration::from_secs(1),
        ));
        assert!(!reg.is_peer_parked(&peer, now));
    }
}
