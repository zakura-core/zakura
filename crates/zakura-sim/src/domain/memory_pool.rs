//! Generic memory-pool gauge properties (`03 §4`, catalogue G1 and G4, `mem-3`,
//! `mem-11`, `mem-13`).
//!
//! [`PoolGaugeModel`] folds [`PoolFact`] observations emitted by the node's
//! [`MemoryPool`](zakura_network::zakura::memory::MemoryPool) observer. It does
//! not restate the pool's admission rules (`decision-15`): it tracks every live
//! lease by ID, verifies conservation against the pool's own totals, checks
//! pool and per-consumer caps (G1 / `mem-3`), checks that every lease returns
//! to zero on teardown (G4 / `mem-11`), and checks that default `Sessions`
//! leases stay within their protected share and that every admitted session
//! runs on a funded connection (`mem-13`). It also joins block-sync leases
//! to the heights they fund and checks that `Consensus` funds each height
//! once (`mem-15`).

use std::collections::{BTreeMap, BTreeSet};

use zakura_network::zakura::memory::{Consumer, LeaseDenied, PoolEvent, Priority};

use crate::{
    domain::block_sync::OwnerView,
    observe::PoolFact,
    property::{
        CheckCtx, HasView, Kind, Pending, Property, PropertyId, Source, Status, TriggerHistory,
        Unit, Verdict,
    },
    Observation, ObservationLog,
};

/// Live leases stay within the pool (`S + floors`) and each consumer's cap at
/// every step, and every grant, resize, split, merge and release conserves
/// bytes (`mem-3`, catalogue G1).
pub const POOL_BOUNDED: &str = "memory/pool-bounded";

/// Once every peer has left and every job has ended, every live lease and
/// every consumer total in the pool (along with block sync's owner charges)
/// returns to zero (`mem-11`, catalogue G4).
pub const TEARDOWN_RETURNS_ZERO: &str = "memory/teardown-returns-zero";

/// With default settings, live `Sessions` leases never exceed `Sessions`'s
/// protected share, no session runs on a connection whose admission still
/// waits for memory, and `Sessions` never waits on pool contention below its
/// floor (`mem-13`).
pub const SESSIONS_WITHIN_SHARE: &str = "memory/sessions-within-share";

/// At rest, `Consensus` holds at most one lease per height. A drain stays on
/// `Consensus` only when the pool had no room to move it to `BlockSync`
/// (`mem-15`).
pub const CONSENSUS_ONCE_PER_HEIGHT: &str = "memory/consensus-once-per-height";

/// Fixed label for `Consumer::Sessions`.
const SESSIONS_LABEL: &str = Consumer::Sessions.label();
/// Fixed label for `Consumer::BlockSync`.
const BLOCK_SYNC_LABEL: &str = Consumer::BlockSync.label();
/// Fixed label for `Consumer::Consensus`.
const CONSENSUS_LABEL: &str = Consumer::Consensus.label();

/// Whether the consumer labelled `consumer` has [`Priority::Reserved`].
pub fn is_reserved_consumer(consumer: &str) -> bool {
    Consumer::ALL
        .iter()
        .any(|spec| spec.label() == consumer && spec.priority() == Priority::Reserved)
}

/// Convert a production [`PoolEvent`] into a serializable [`PoolFact`].
pub fn fact_from_event(event: &PoolEvent) -> PoolFact {
    match event {
        PoolEvent::Granted {
            lease_id,
            consumer,
            peer,
            bytes,
            consumer_leased,
            consumer_cap,
            consumer_share,
            total_leased,
            pool_limit,
        } => PoolFact::Granted {
            lease_id: *lease_id,
            consumer: consumer.label().to_owned(),
            peer: *peer,
            bytes: *bytes,
            consumer_leased: *consumer_leased,
            consumer_cap: *consumer_cap,
            consumer_share: *consumer_share,
            total_leased: *total_leased,
            pool_limit: *pool_limit,
        },
        PoolEvent::Resized {
            lease_id,
            consumer,
            peer,
            old_bytes,
            new_bytes,
            consumer_leased,
            consumer_cap,
            consumer_share,
            total_leased,
            pool_limit,
        } => PoolFact::Resized {
            lease_id: *lease_id,
            consumer: consumer.label().to_owned(),
            peer: *peer,
            old_bytes: *old_bytes,
            new_bytes: *new_bytes,
            consumer_leased: *consumer_leased,
            consumer_cap: *consumer_cap,
            consumer_share: *consumer_share,
            total_leased: *total_leased,
            pool_limit: *pool_limit,
        },
        PoolEvent::Split {
            from_lease_id,
            to_lease_id,
            consumer,
            peer,
            bytes,
        } => PoolFact::Split {
            from_lease_id: *from_lease_id,
            to_lease_id: *to_lease_id,
            consumer: consumer.label().to_owned(),
            peer: *peer,
            bytes: *bytes,
        },
        PoolEvent::Merged {
            into_lease_id,
            from_lease_id,
            consumer,
            peer,
            bytes,
        } => PoolFact::Merged {
            into_lease_id: *into_lease_id,
            from_lease_id: *from_lease_id,
            consumer: consumer.label().to_owned(),
            peer: *peer,
            bytes: *bytes,
        },
        PoolEvent::Moved {
            from_lease_id,
            to_lease_id,
            from,
            to,
            peer,
            bytes,
            from_leased,
            to_leased,
            total_leased,
            pool_limit,
        } => PoolFact::Moved {
            from_lease_id: *from_lease_id,
            to_lease_id: *to_lease_id,
            from: from.label().to_owned(),
            to: to.label().to_owned(),
            peer: *peer,
            bytes: *bytes,
            from_leased: *from_leased,
            to_leased: *to_leased,
            total_leased: *total_leased,
            pool_limit: *pool_limit,
        },
        PoolEvent::Released {
            lease_id,
            consumer,
            peer,
            bytes,
            consumer_leased,
            total_leased,
            pool_limit,
        } => PoolFact::Released {
            lease_id: *lease_id,
            consumer: consumer.label().to_owned(),
            peer: *peer,
            bytes: *bytes,
            consumer_leased: *consumer_leased,
            total_leased: *total_leased,
            pool_limit: *pool_limit,
        },
        PoolEvent::Labeled { lease_id, height } => PoolFact::Labeled {
            lease_id: *lease_id,
            height: *height,
        },
        PoolEvent::Waited {
            consumer,
            peer,
            requested_bytes,
            reason,
        } => PoolFact::Waited {
            consumer: consumer.label().to_owned(),
            peer: *peer,
            requested_bytes: *requested_bytes,
            reason: denial_label(*reason).to_owned(),
        },
    }
}

/// Stable kebab-case label for [`LeaseDenied`].
pub const fn denial_label(reason: LeaseDenied) -> &'static str {
    match reason {
        LeaseDenied::CapExceeded => "cap-exceeded",
        LeaseDenied::PoolFull => "pool-full",
        LeaseDenied::BelowShareWaiting => "below-share-waiting",
        LeaseDenied::ShareExceeded => "share-exceeded",
    }
}

/// Live lease gauge folded from [`PoolFact`] observations.
#[derive(Clone, Debug)]
pub struct PoolGaugeModel {
    /// Every live lease, by `lease_id`: `(consumer_label, bytes)`.
    leases: BTreeMap<u64, (String, u64)>,
    /// Live leased bytes per consumer, summed from `leases`.
    by_consumer: BTreeMap<String, u64>,
    /// Live leased bytes across every consumer.
    total_leased: u64,
    /// Peak leased bytes per consumer over the run.
    peaks_by_consumer: BTreeMap<String, u64>,
    /// Peak total leased bytes over the run.
    peak_total: u64,
    /// Latest pool limit (`total_floors + S`) reported by the pool.
    pool_limit: Option<u64>,
    /// Latest effective cap reported per consumer.
    last_cap: BTreeMap<String, u64>,
    /// The cap each consumer's latest peak was granted under.
    caps_at_peak: BTreeMap<String, u64>,
    /// Latest protected share reported per consumer.
    last_share: BTreeMap<String, u64>,
    /// Configured `Sessions` floor bytes for `mem-13` floor preservation.
    sessions_floor: u64,
    /// Whether the node runs with default session settings (so `Sessions` must
    /// never exceed its protected share).
    default_session_settings: bool,
    /// Total `PoolFact` events folded.
    events_observed: u64,
    /// Total `Sessions` events folded.
    sessions_events_observed: u64,
    /// Connections whose `Sessions` admission waited on pool contention and
    /// has no grant yet. A cap or share refusal is final and reports no
    /// wait, but it closes the connection before registration, so no
    /// session can follow it.
    sessions_waiting_conns: BTreeSet<u64>,
    /// Connections on which at least one service session was admitted.
    admitted_session_conns: BTreeSet<u64>,
    /// G1 (`memory/pool-bounded`) violations found while folding.
    bound_violations: Vec<(String, String)>,
    /// `mem-13` (`memory/sessions-within-share`) violations found while folding.
    session_violations: Vec<(String, String)>,
    /// `PoolFact::Waited` events recorded on reserved consumers (for `mem-5`).
    waited_reserved: Vec<(String, u64, String)>,
    /// The height each labeled live lease funds, by `lease_id`. `None` marks
    /// a drain a move left behind (`mem-15`).
    heights: BTreeMap<u64, Option<u32>>,
}

impl Default for PoolGaugeModel {
    fn default() -> Self {
        Self::new(0, true)
    }
}

impl PoolGaugeModel {
    /// An empty gauge model configured with `sessions_floor` and whether the
    /// node uses default session settings.
    pub fn new(sessions_floor: u64, default_session_settings: bool) -> Self {
        Self {
            leases: BTreeMap::new(),
            by_consumer: BTreeMap::new(),
            total_leased: 0,
            peaks_by_consumer: BTreeMap::new(),
            peak_total: 0,
            pool_limit: None,
            last_cap: BTreeMap::new(),
            caps_at_peak: BTreeMap::new(),
            last_share: BTreeMap::new(),
            sessions_floor,
            default_session_settings,
            events_observed: 0,
            sessions_events_observed: 0,
            sessions_waiting_conns: BTreeSet::new(),
            admitted_session_conns: BTreeSet::new(),
            bound_violations: Vec::new(),
            session_violations: Vec::new(),
            waited_reserved: Vec::new(),
            heights: BTreeMap::new(),
        }
    }

    /// Fold one [`PoolFact`] into the gauge.
    pub fn observe(&mut self, fact: &PoolFact) {
        self.events_observed = self.events_observed.saturating_add(1);
        match fact {
            PoolFact::Granted {
                lease_id,
                consumer,
                peer,
                bytes,
                consumer_leased,
                consumer_cap,
                consumer_share,
                total_leased,
                pool_limit,
                ..
            } => {
                if consumer == SESSIONS_LABEL {
                    self.sessions_events_observed = self.sessions_events_observed.saturating_add(1);
                }
                if self
                    .leases
                    .insert(*lease_id, (consumer.clone(), *bytes))
                    .is_some()
                {
                    self.bound_violations.push((
                        "each granted lease carries a fresh lease_id".to_owned(),
                        format!(
                            "lease_id {lease_id} for consumer `{consumer}` ({bytes} B) was already live"
                        ),
                    ));
                }
                self.add_bytes(consumer, *bytes);
                if consumer == SESSIONS_LABEL {
                    if let Some(conn_id) = peer {
                        self.sessions_waiting_conns.remove(conn_id);
                    }
                }
                self.check_reported_bounds(
                    consumer,
                    *consumer_leased,
                    Some((*consumer_cap, *consumer_share, true)),
                    *total_leased,
                    *pool_limit,
                );
            }
            PoolFact::Resized {
                lease_id,
                consumer,
                old_bytes,
                new_bytes,
                consumer_leased,
                consumer_cap,
                consumer_share,
                total_leased,
                pool_limit,
                ..
            } => {
                if consumer == SESSIONS_LABEL {
                    self.sessions_events_observed = self.sessions_events_observed.saturating_add(1);
                }
                match self.leases.get_mut(lease_id) {
                    Some((live_consumer, live_bytes)) => {
                        if live_consumer != consumer || *live_bytes != *old_bytes {
                            self.bound_violations.push((
                                format!(
                                    "resize of lease {lease_id} matches live consumer `{live_consumer}` and {live_bytes} B"
                                ),
                                format!(
                                    "reported `{consumer}` resizing from {old_bytes} B to {new_bytes} B"
                                ),
                            ));
                        }
                        if *new_bytes >= *old_bytes {
                            let delta = new_bytes - old_bytes;
                            *live_bytes = *new_bytes;
                            self.add_bytes(consumer, delta);
                        } else {
                            let delta = old_bytes - new_bytes;
                            *live_bytes = *new_bytes;
                            self.sub_bytes(consumer, delta);
                        }
                    }
                    None => {
                        self.bound_violations.push((
                            "only a live lease is resized".to_owned(),
                            format!(
                                "lease_id {lease_id} for `{consumer}` resized from {old_bytes} to {new_bytes} without being live"
                            ),
                        ));
                    }
                }
                // `set_cap` may lower a cap below what a consumer holds, so
                // only a resize that grows the lease must fit the cap.
                self.check_reported_bounds(
                    consumer,
                    *consumer_leased,
                    Some((*consumer_cap, *consumer_share, new_bytes > old_bytes)),
                    *total_leased,
                    *pool_limit,
                );
            }
            PoolFact::Split {
                from_lease_id,
                to_lease_id,
                consumer,
                bytes,
                ..
            } => {
                if consumer == SESSIONS_LABEL {
                    self.sessions_events_observed = self.sessions_events_observed.saturating_add(1);
                }
                match self.leases.get_mut(from_lease_id) {
                    Some((live_consumer, live_bytes))
                        if live_consumer == consumer && *live_bytes >= *bytes =>
                    {
                        *live_bytes -= *bytes;
                        if *live_bytes == 0 {
                            self.leases.remove(from_lease_id);
                        }
                    }
                    other => {
                        self.bound_violations.push((
                            format!(
                                "split of {bytes} B from lease {from_lease_id} (`{consumer}`) takes from a live lease with at least {bytes} B"
                            ),
                            format!("source lease state was {other:?}"),
                        ));
                    }
                }
                if let Some(height) = self.heights.get(from_lease_id).copied() {
                    self.heights.insert(*to_lease_id, height);
                }
                if self
                    .leases
                    .insert(*to_lease_id, (consumer.clone(), *bytes))
                    .is_some()
                {
                    self.bound_violations.push((
                        "split creates a child lease with a fresh lease_id".to_owned(),
                        format!("child lease_id {to_lease_id} for `{consumer}` was already live"),
                    ));
                }
            }
            PoolFact::Merged {
                into_lease_id,
                from_lease_id,
                consumer,
                bytes,
                ..
            } => {
                if consumer == SESSIONS_LABEL {
                    self.sessions_events_observed = self.sessions_events_observed.saturating_add(1);
                }
                match self.leases.remove(from_lease_id) {
                    Some((from_consumer, from_bytes))
                        if from_consumer == *consumer && from_bytes == *bytes => {}
                    other => {
                        self.bound_violations.push((
                            format!(
                                "merge consumes live lease {from_lease_id} (`{consumer}`, {bytes} B)"
                            ),
                            format!("removed source lease was {other:?}"),
                        ));
                    }
                }
                if let Some(height) = self.heights.remove(from_lease_id) {
                    self.heights.entry(*into_lease_id).or_insert(height);
                }
                match self.leases.get_mut(into_lease_id) {
                    Some((into_consumer, into_bytes)) if into_consumer == consumer => {
                        *into_bytes = into_bytes.saturating_add(*bytes);
                    }
                    other => {
                        self.bound_violations.push((
                            format!("merge target lease {into_lease_id} (`{consumer}`) is live"),
                            format!("target lease was {other:?}"),
                        ));
                    }
                }
            }
            PoolFact::Moved {
                from_lease_id,
                to_lease_id,
                from,
                to,
                bytes,
                from_leased,
                to_leased,
                total_leased,
                pool_limit,
                ..
            } => {
                match self.leases.get_mut(from_lease_id) {
                    Some((live_consumer, live_bytes))
                        if live_consumer == from && *live_bytes >= *bytes =>
                    {
                        *live_bytes -= *bytes;
                        if *live_bytes == 0 {
                            self.leases.remove(from_lease_id);
                        }
                    }
                    other => {
                        self.bound_violations.push((
                            format!(
                                "move of {bytes} B from lease {from_lease_id} (`{from}`) takes from a live lease with at least {bytes} B"
                            ),
                            format!("source lease state was {other:?}"),
                        ));
                    }
                }
                if let Some(height) = self.heights.get(from_lease_id).copied() {
                    self.heights.insert(*to_lease_id, height);
                }
                if self
                    .leases
                    .insert(*to_lease_id, (to.clone(), *bytes))
                    .is_some()
                {
                    self.bound_violations.push((
                        "a move creates a lease with a fresh lease_id".to_owned(),
                        format!("lease_id {to_lease_id} for `{to}` was already live"),
                    ));
                }
                self.sub_bytes(from, *bytes);
                self.add_bytes(to, *bytes);
                // A move adds no ledger bytes, so it checks totals but not
                // the target's cap.
                self.check_reported_bounds(from, *from_leased, None, *total_leased, *pool_limit);
                self.check_reported_bounds(to, *to_leased, None, *total_leased, *pool_limit);
            }
            PoolFact::Released {
                lease_id,
                consumer,
                bytes,
                consumer_leased,
                total_leased,
                pool_limit,
                ..
            } => {
                if consumer == SESSIONS_LABEL {
                    self.sessions_events_observed = self.sessions_events_observed.saturating_add(1);
                }
                self.heights.remove(lease_id);
                match self.leases.remove(lease_id) {
                    Some((live_consumer, live_bytes)) => {
                        if live_consumer != *consumer || live_bytes != *bytes {
                            self.bound_violations.push((
                                format!(
                                    "released lease {lease_id} matches live consumer `{live_consumer}` and {live_bytes} B"
                                ),
                                format!("released `{consumer}` with {bytes} B"),
                            ));
                        }
                        self.sub_bytes(consumer, *bytes);
                    }
                    None => {
                        self.bound_violations.push((
                            "only a live lease is released".to_owned(),
                            format!(
                                "lease_id {lease_id} for `{consumer}` ({bytes} B) released without being live"
                            ),
                        ));
                    }
                }
                self.check_reported_bounds(
                    consumer,
                    *consumer_leased,
                    None,
                    *total_leased,
                    *pool_limit,
                );
            }
            PoolFact::Labeled { lease_id, height } => {
                self.heights.insert(*lease_id, *height);
            }
            PoolFact::Waited {
                consumer,
                peer,
                requested_bytes,
                reason,
            } => {
                if is_reserved_consumer(consumer) {
                    self.waited_reserved
                        .push((consumer.clone(), *requested_bytes, reason.clone()));
                }
                if consumer == SESSIONS_LABEL {
                    self.sessions_events_observed = self.sessions_events_observed.saturating_add(1);
                    if let Some(conn_id) = peer {
                        self.sessions_waiting_conns.insert(*conn_id);
                        if self.admitted_session_conns.contains(conn_id) {
                            self.session_violations.push((
                                "a connection waits for Sessions memory only before it admits a session".to_owned(),
                                format!(
                                    "connection {conn_id} admitted service sessions, then its Sessions admission waited ({reason})"
                                ),
                            ));
                        }
                    }
                    // Floor preservation: live sessions must never be blocked
                    // below floor by pool contention (`pool-full` or
                    // `below-share-waiting`).
                    let current = self.leased(SESSIONS_LABEL);
                    if matches!(reason.as_str(), "pool-full" | "below-share-waiting")
                        && current.saturating_add(*requested_bytes) <= self.sessions_floor
                    {
                        self.session_violations.push((
                            format!(
                                "Sessions leases up to floor ({} B) are never blocked by pool contention",
                                self.sessions_floor
                            ),
                            format!(
                                "Sessions waited with `{reason}` while requesting {requested_bytes} B at {current} B leased"
                            ),
                        ));
                    }
                }
            }
        }
    }

    /// Record that a service session was admitted on `connection`.
    ///
    /// A session on a connection whose `Sessions` admission waited and was
    /// never granted violates `mem-13`: the node admitted reactors that
    /// memory didn't fund.
    pub fn observe_session(&mut self, connection: u64) {
        self.admitted_session_conns.insert(connection);
        if self.sessions_waiting_conns.contains(&connection) {
            self.session_violations.push((
                "the node never admits a session on a connection memory didn't fund".to_owned(),
                format!(
                    "connection {connection} admitted a service session while its Sessions \
                     admission still waited"
                ),
            ));
        }
    }

    fn add_bytes(&mut self, consumer: &str, bytes: u64) {
        let entry = self.by_consumer.entry(consumer.to_owned()).or_default();
        *entry = entry.saturating_add(bytes);
        let c_val = *entry;
        let peak = self
            .peaks_by_consumer
            .entry(consumer.to_owned())
            .or_default();
        *peak = (*peak).max(c_val);
        self.total_leased = self.total_leased.saturating_add(bytes);
        self.peak_total = self.peak_total.max(self.total_leased);
    }

    fn sub_bytes(&mut self, consumer: &str, bytes: u64) {
        let entry = self.by_consumer.entry(consumer.to_owned()).or_default();
        if *entry < bytes {
            self.bound_violations.push((
                format!("consumer `{consumer}` never releases more bytes than it holds"),
                format!("held {} B, released {bytes} B", *entry),
            ));
        }
        *entry = entry.saturating_sub(bytes);
        if self.total_leased < bytes {
            self.bound_violations.push((
                "pool total never underflows on release".to_owned(),
                format!("total {} B, released {bytes} B", self.total_leased),
            ));
        }
        self.total_leased = self.total_leased.saturating_sub(bytes);
    }

    /// Check the totals an event reports against the folded leases.
    /// `limits` carries the reported cap and share, and whether the event
    /// grew the consumer's leases. Only growth must fit them: `set_cap` may
    /// lower a cap below what a consumer holds, and a later shrink still
    /// reports the lower cap.
    fn check_reported_bounds(
        &mut self,
        consumer: &str,
        reported_consumer: u64,
        limits: Option<(u64, u64, bool)>,
        reported_total: u64,
        pool_limit: u64,
    ) {
        self.pool_limit = Some(pool_limit);
        let folded_consumer = self.leased(consumer);
        if folded_consumer != reported_consumer {
            self.bound_violations.push((
                format!(
                    "folded live leases for `{consumer}` ({folded_consumer} B) match reported consumer total"
                ),
                format!("pool reported {reported_consumer} B"),
            ));
        }
        if self.total_leased != reported_total {
            self.bound_violations.push((
                format!(
                    "folded pool total ({} B) matches reported total",
                    self.total_leased
                ),
                format!("pool reported {reported_total} B"),
            ));
        }
        if reported_total > pool_limit || self.total_leased > pool_limit {
            self.bound_violations.push((
                format!("live pool leases stay within S + floors ({pool_limit} B)"),
                format!(
                    "reported total {reported_total} B (folded {} B)",
                    self.total_leased
                ),
            ));
        }
        let Some((cap, share, grew)) = limits else {
            return;
        };
        self.last_cap.insert(consumer.to_owned(), cap);
        self.last_share.insert(consumer.to_owned(), share);
        if !grew {
            return;
        }
        if self.peaks_by_consumer.get(consumer) == Some(&folded_consumer) {
            self.caps_at_peak.insert(consumer.to_owned(), cap);
        }
        if reported_consumer > cap || folded_consumer > cap {
            self.bound_violations.push((
                format!("consumer `{consumer}` stays within its cap ({cap} B)"),
                format!("reported {reported_consumer} B (folded {folded_consumer} B)"),
            ));
        }
        if consumer == SESSIONS_LABEL
            && self.default_session_settings
            && (reported_consumer > share || folded_consumer > share)
        {
            self.session_violations.push((
                format!(
                    "with default settings, live Sessions leases stay within share ({share} B)"
                ),
                format!("Sessions leased {reported_consumer} B (folded {folded_consumer} B)"),
            ));
        }
    }

    /// Live leased bytes for `consumer` (0 if none).
    pub fn leased(&self, consumer: &str) -> u64 {
        self.by_consumer.get(consumer).copied().unwrap_or(0)
    }

    /// Combined live leased bytes for `BlockSync` + `Consensus` (block sync's
    /// requests and retained bodies across commit-window and look-ahead).
    pub fn block_sync_total_leased(&self) -> u64 {
        self.leased(BLOCK_SYNC_LABEL)
            .saturating_add(self.leased(CONSENSUS_LABEL))
    }

    /// Total live leased bytes across every consumer.
    pub fn total_leased(&self) -> u64 {
        self.total_leased
    }

    /// Number of currently live leases tracked by ID.
    pub fn live_lease_count(&self) -> usize {
        self.leases.len()
    }

    /// Every consumer with a non-zero live balance.
    pub fn nonzero_consumers(&self) -> Vec<(String, u64)> {
        self.by_consumer
            .iter()
            .filter(|(_, bytes)| **bytes > 0)
            .map(|(consumer, bytes)| (consumer.clone(), *bytes))
            .collect()
    }

    /// Peak leased bytes recorded per consumer.
    pub fn peaks_by_consumer(&self) -> &BTreeMap<String, u64> {
        &self.peaks_by_consumer
    }

    /// Peak total leased bytes recorded across the pool.
    pub fn peak_total(&self) -> u64 {
        self.peak_total
    }

    /// Latest reported pool limit (`total_floors + S`).
    pub fn pool_limit(&self) -> Option<u64> {
        self.pool_limit
    }

    /// Latest reported cap for `consumer`.
    pub fn last_cap(&self, consumer: &str) -> Option<u64> {
        self.last_cap.get(consumer).copied()
    }

    /// The cap each consumer's peak was granted under.
    pub fn caps_at_peak(&self) -> &BTreeMap<String, u64> {
        &self.caps_at_peak
    }

    /// Latest reported protected share for `consumer`.
    pub fn last_share(&self, consumer: &str) -> Option<u64> {
        self.last_share.get(consumer).copied()
    }

    /// Total `PoolFact` events folded.
    pub fn events_observed(&self) -> u64 {
        self.events_observed
    }

    /// Total `Sessions` events folded.
    pub fn sessions_events_observed(&self) -> u64 {
        self.sessions_events_observed
    }

    /// G1 (`memory/pool-bounded`) violations folded so far.
    pub fn bound_violations(&self) -> &[(String, String)] {
        &self.bound_violations
    }

    /// `mem-13` (`memory/sessions-within-share`) violations folded so far.
    pub fn session_violations(&self) -> &[(String, String)] {
        &self.session_violations
    }

    /// Reserved-consumer waits folded so far (`(consumer, requested_bytes, reason)`).
    pub fn waited_reserved(&self) -> &[(String, u64, String)] {
        &self.waited_reserved
    }

    /// The heights the live `Consensus` leases fund, one entry per lease
    /// that funds a height, and the first live `Consensus` lease that names
    /// no height. A drain a move left behind funds none (`mem-15`).
    pub fn consensus_heights(&self) -> (Vec<(u32, u64)>, Option<u64>) {
        let mut heights = Vec::new();
        let mut unlabeled = None;
        for (lease_id, (consumer, bytes)) in &self.leases {
            if consumer != CONSENSUS_LABEL || *bytes == 0 {
                continue;
            }
            match self.heights.get(lease_id) {
                Some(Some(height)) => heights.push((*height, *lease_id)),
                Some(None) => {}
                None => {
                    unlabeled.get_or_insert(*lease_id);
                }
            }
        }
        heights.sort_unstable();
        (heights, unlabeled)
    }

    /// Fold every `Observation::Pool` and `Observation::Session` in `log`.
    pub fn of_log(
        log: &ObservationLog,
        sessions_floor: u64,
        default_session_settings: bool,
    ) -> Self {
        let mut model = Self::new(sessions_floor, default_session_settings);
        for entry in log.entries() {
            match &entry.obs {
                Observation::Pool { event, .. } => model.observe(event),
                Observation::Session { connection, .. } => model.observe_session(*connection),
                _ => {}
            }
        }
        model
    }
}

/// A harness view carrying a [`PoolGaugeModel`].
pub trait PoolView {
    /// Runs `read` on the pool gauge model after a step.
    fn read_pool<T>(&self, read: impl FnOnce(&PoolGaugeModel) -> T) -> T;
}

/// [`POOL_BOUNDED`] (`mem-3`, catalogue G1): live leases stay within
/// `S + floors` and each consumer's cap at every step.
pub fn pool_bounded<S: HasView>() -> Property<S>
where
    S::View: PoolView,
{
    Property {
        id: PropertyId::new(POOL_BOUNDED),
        claim: "live memory-pool leases stay within S + floors and each consumer's cap at every \
                step, and every grant, resize, split, merge and release conserves bytes",
        kind: Kind::Bound {
            unit: Unit::ResidentBytes,
        },
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: check_pool_bounded::<S>,
        witness: Some(TriggerHistory::Log(pool_lease_observed)),
    }
}

/// [`TEARDOWN_RETURNS_ZERO`] (`mem-11`, catalogue G4): replaces
/// `block-sync/teardown-returns-baseline`. Once every peer has left, every
/// routine has ended, the input queue is empty and every serving lease is
/// released, at rest, every live pool lease and every block-sync charge is
/// back at zero.
pub fn teardown_returns_zero<S: HasView>() -> Property<S>
where
    S::View: PoolView + OwnerView,
{
    Property {
        id: PropertyId::new(TEARDOWN_RETURNS_ZERO),
        claim: "once every peer has left, every routine has ended, the input queue is empty and \
                every serving lease is released, at rest, every live pool lease and every \
                block-sync charge returns to zero",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View, Source::Schedule],
        status: Status::Active,
        check: check_teardown_returns_zero::<S>,
        witness: Some(TriggerHistory::Log(pool_teardown_triggered)),
    }
}

/// [`SESSIONS_WITHIN_SHARE`] (`mem-13`): with default settings, live
/// transport and session leases never exceed `Sessions`'s share, no session
/// runs on a connection whose admission still waits for memory, and
/// `Sessions` never waits on pool contention below its floor.
pub fn sessions_within_share<S: HasView>() -> Property<S>
where
    S::View: PoolView,
{
    Property {
        id: PropertyId::new(SESSIONS_WITHIN_SHARE),
        claim: "with default settings, live transport and reactor-session leases never exceed the \
                sessions consumer's protected share, no session runs on a connection whose \
                admission still waits for memory, and the sessions consumer never waits on pool \
                contention below its floor",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: check_sessions_within_share::<S>,
        witness: Some(TriggerHistory::Log(sessions_lease_observed)),
    }
}

/// [`CONSENSUS_ONCE_PER_HEIGHT`] (`mem-15`): at rest, every live `Consensus`
/// lease names the height it funds, and no two name the same one. A drain
/// that a move left on `Consensus` names none.
///
/// The check runs at rest because the pool's lock is a scheduling point: a
/// step can end between a grant and its label, or between a split and the
/// move of its drain. Block sync panics in the simulator when `Consensus`
/// refuses a commit-window body at decode, so a run that ends without that
/// panic also shows that every decoded commit-window body holds a
/// `Consensus` lease.
pub fn consensus_once_per_height<S: HasView>() -> Property<S>
where
    S::View: PoolView,
{
    Property {
        id: PropertyId::new(CONSENSUS_ONCE_PER_HEIGHT),
        claim: "at rest, every live consensus lease funds one height, no two fund the same \
                height, and a drain stays on consensus only when the pool had no room to move it \
                to block sync",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View, Source::Schedule],
        status: Status::Active,
        check: check_consensus_once_per_height::<S>,
        witness: Some(TriggerHistory::Log(consensus_height_reissued)),
    }
}

fn check_consensus_once_per_height<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S::View: PoolView,
{
    if !crate::at_rest(cx.status) {
        return Verdict::NotApplicable;
    }
    let (heights, unlabeled) = cx.view().read_pool(PoolGaugeModel::consensus_heights);
    if let Some(lease_id) = unlabeled {
        return cx.violated(
            "every live consensus lease names the height it funds",
            format!("consensus lease {lease_id} names none"),
        );
    }
    if heights.is_empty() {
        return Verdict::NotApplicable;
    }
    cx.peak(
        "consensus-leases",
        u64::try_from(heights.len()).unwrap_or(u64::MAX),
    );
    if let Some(pair) = heights.windows(2).find(|pair| pair[0].0 == pair[1].0) {
        let ((height, first), (_, second)) = (pair[0], pair[1]);
        return cx.violated(
            format!("consensus holds at most one lease for height {height}"),
            format!("leases {first} and {second} both fund it"),
        );
    }
    Verdict::Holds
}

/// [`CONSENSUS_ONCE_PER_HEIGHT`]'s trigger history: two `Consensus` leases
/// funded the same height over the run, as a re-issued request or a late
/// body's lease does.
pub fn consensus_height_reissued(log: &ObservationLog) -> bool {
    let mut consensus = BTreeSet::new();
    let mut funded = BTreeSet::new();
    for entry in log.entries() {
        match &entry.obs {
            Observation::Pool {
                event:
                    PoolFact::Granted {
                        lease_id, consumer, ..
                    },
                ..
            } if consumer == CONSENSUS_LABEL => {
                consensus.insert(*lease_id);
            }
            Observation::Pool {
                event:
                    PoolFact::Labeled {
                        lease_id,
                        height: Some(height),
                    },
                ..
            } if consensus.contains(lease_id) && !funded.insert(*height) => return true,
            _ => {}
        }
    }
    false
}

fn check_pool_bounded<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S::View: PoolView,
{
    let (observed, peak_total, peaks_by_consumer, violations) = cx.view().read_pool(|pool| {
        (
            pool.events_observed(),
            pool.peak_total(),
            pool.peaks_by_consumer().clone(),
            pool.bound_violations().to_vec(),
        )
    });
    if observed == 0 {
        return Verdict::NotApplicable;
    }
    cx.peak("pool-leased-bytes", peak_total);
    for (label, key) in [
        ("block_sync", "block-sync-leased-bytes"),
        ("consensus", "consensus-leased-bytes"),
        ("serving", "serving-leased-bytes"),
        ("sessions", "sessions-leased-bytes"),
        ("tx", "tx-leased-bytes"),
    ] {
        if let Some(&peak) = peaks_by_consumer.get(label) {
            cx.peak(key, peak);
        }
    }
    let mut violations = violations.into_iter();
    if let Some((expected, actual)) = violations.next() {
        for (exp, act) in violations {
            cx.record_violation(exp, act);
        }
        return cx.violated(expected, actual);
    }
    Verdict::Holds
}

fn check_teardown_returns_zero<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S::View: PoolView + OwnerView,
{
    let status = cx.status;
    let (left, unended, queued, serving, charges, gap) = cx.view().read_owners(|owners| {
        (
            owners.peers_left(),
            owners.unended(),
            owners.queued(),
            owners.serving_leases(),
            [
                ("the byte budget", owners.reserved()),
                ("the work queue's tally", owners.queue_reserved()),
                (
                    "the routines' unreceived estimates",
                    owners.unreceived_estimates(),
                ),
                (
                    "open handoffs",
                    u64::try_from(owners.handoffs().len()).unwrap_or(u64::MAX),
                ),
                ("held session slots", owners.session_slots()),
            ],
            owners
                .gap()
                .or_else(|| owners.serving_gap().map(str::to_owned)),
        )
    });
    if !left {
        return Verdict::NotApplicable;
    }
    if let Some(gap) = gap {
        cx.missing_evidence(gap);
        return Verdict::NotApplicable;
    }
    if unended > 0 || queued > 0 || serving > 0 || !crate::at_rest(status) {
        cx.pending(Pending::BlockTeardownOpen);
        return Verdict::NotApplicable;
    }

    let (total_leased, live_leases, nonzero) = cx.view().read_pool(|pool| {
        (
            pool.total_leased(),
            pool.live_lease_count(),
            pool.nonzero_consumers(),
        )
    });
    if total_leased > 0 || live_leases > 0 {
        return cx.violated(
            "every memory-pool lease returns to zero once teardown settles",
            format!(
                "pool still holds {total_leased} B across {live_leases} live leases ({nonzero:?})"
            ),
        );
    }
    match charges.iter().find(|(_, value)| *value > 0) {
        None => Verdict::Holds,
        Some((owner, value)) => cx.violated(
            format!("{owner} is zero once teardown settles"),
            format!("it counts {value}"),
        ),
    }
}

fn check_sessions_within_share<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S::View: PoolView,
{
    let (observed, peak_sessions, violations) = cx.view().read_pool(|pool| {
        (
            pool.sessions_events_observed(),
            pool.peaks_by_consumer()
                .get(SESSIONS_LABEL)
                .copied()
                .unwrap_or(0),
            pool.session_violations().to_vec(),
        )
    });
    if observed == 0 {
        return Verdict::NotApplicable;
    }
    cx.peak("sessions-leased-bytes", peak_sessions);
    let mut violations = violations.into_iter();
    if let Some((expected, actual)) = violations.next() {
        for (exp, act) in violations {
            cx.record_violation(exp, act);
        }
        return cx.violated(expected, actual);
    }
    Verdict::Holds
}

/// [`POOL_BOUNDED`]'s trigger history: the pool recorded at least one lease
/// grant, resize, or release.
pub fn pool_lease_observed(log: &ObservationLog) -> bool {
    log.entries().iter().any(|entry| {
        matches!(
            entry.obs,
            Observation::Pool {
                event: PoolFact::Granted { .. }
                    | PoolFact::Resized { .. }
                    | PoolFact::Released { .. },
                ..
            }
        )
    })
}

/// [`TEARDOWN_RETURNS_ZERO`]'s trigger history: either a block-sync routine
/// ended owing requests, or the pool granted and released at least one lease.
pub fn pool_teardown_triggered(log: &ObservationLog) -> bool {
    let mut granted = false;
    let mut released = false;
    for entry in log.entries() {
        match &entry.obs {
            Observation::BlockSync {
                event:
                    crate::observe::BlockSyncFact::Routine {
                        ended: true,
                        requests,
                        ..
                    },
                ..
            } if !requests.is_empty() => return true,
            Observation::Pool {
                event: PoolFact::Granted { .. },
                ..
            } => granted = true,
            Observation::Pool {
                event: PoolFact::Released { .. },
                ..
            } => released = true,
            _ => {}
        }
    }
    granted && released
}

/// [`SESSIONS_WITHIN_SHARE`]'s trigger history: the pool recorded at least one
/// `Sessions` event.
pub fn sessions_lease_observed(log: &ObservationLog) -> bool {
    log.entries().iter().any(|entry| {
        matches!(
            &entry.obs,
            Observation::Pool {
                event: PoolFact::Granted { consumer, .. }
                    | PoolFact::Resized { consumer, .. }
                    | PoolFact::Released { consumer, .. }
                    | PoolFact::Waited { consumer, .. },
                ..
            } if consumer == SESSIONS_LABEL
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A grant of `bytes` to `consumer` as lease `lease_id`.
    fn granted(lease_id: u64, consumer: &str, bytes: u64) -> PoolFact {
        PoolFact::Granted {
            lease_id,
            consumer: consumer.to_owned(),
            peer: Some(1),
            bytes,
            consumer_leased: bytes,
            consumer_cap: u64::MAX,
            consumer_share: u64::MAX,
            total_leased: bytes,
            pool_limit: u64::MAX,
        }
    }

    /// `lease_id` names `height`.
    fn labeled(lease_id: u64, height: Option<u32>) -> PoolFact {
        PoolFact::Labeled { lease_id, height }
    }

    /// Two consensus leases on one height show as two entries; a drain the
    /// move left behind shows as none; an unlabeled lease is reported.
    #[test]
    fn consensus_heights_join_labels_to_live_leases() {
        let mut pool = PoolGaugeModel::default();
        pool.observe(&granted(1, CONSENSUS_LABEL, 10));
        assert_eq!(pool.consensus_heights(), (Vec::new(), Some(1)));
        pool.observe(&labeled(1, Some(7)));
        pool.observe(&granted(2, CONSENSUS_LABEL, 10));
        pool.observe(&labeled(2, Some(7)));
        assert_eq!(pool.consensus_heights(), (vec![(7, 1), (7, 2)], None));
        pool.observe(&labeled(2, None));
        assert_eq!(pool.consensus_heights(), (vec![(7, 1)], None));
        pool.observe(&granted(3, BLOCK_SYNC_LABEL, 10));
        pool.observe(&labeled(3, Some(7)));
        assert_eq!(pool.consensus_heights(), (vec![(7, 1)], None));
    }

    /// A split child keeps its parent's height, so a drain split from a
    /// commit-window lease and never moved shows as a second lease on it.
    #[test]
    fn a_split_drain_keeps_its_height_until_it_moves() {
        let mut pool = PoolGaugeModel::default();
        pool.observe(&granted(1, CONSENSUS_LABEL, 10));
        pool.observe(&labeled(1, Some(7)));
        pool.observe(&PoolFact::Split {
            from_lease_id: 1,
            to_lease_id: 2,
            consumer: CONSENSUS_LABEL.to_owned(),
            peer: Some(1),
            bytes: 4,
        });
        assert_eq!(pool.consensus_heights(), (vec![(7, 1), (7, 2)], None));
        pool.observe(&PoolFact::Moved {
            from_lease_id: 2,
            to_lease_id: 3,
            from: CONSENSUS_LABEL.to_owned(),
            to: BLOCK_SYNC_LABEL.to_owned(),
            peer: Some(1),
            bytes: 4,
            from_leased: 6,
            to_leased: 4,
            total_leased: 10,
            pool_limit: u64::MAX,
        });
        assert_eq!(pool.consensus_heights(), (vec![(7, 1)], None));
    }
}
