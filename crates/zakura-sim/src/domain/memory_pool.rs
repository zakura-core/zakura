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

use zakura_network::zakura::memory::{Consumer, LeaseDenied, Priority};

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

/// With bulk consumers saturated, reserved tip-priority consumers (`control`,
/// `propagation`, `consensus`) reach their bounds without waiting (`mem-5`).
pub const TIP_NEVER_WAITS: &str = "memory/tip-never-waits";

/// At rest, `Consensus` holds at most one lease per height. A drain stays on
/// `Consensus` only when the pool had no room to move it to `BlockSync`
/// (`mem-15`).
pub const CONSENSUS_ONCE_PER_HEIGHT: &str = "memory/consensus-once-per-height";

/// Live lease gauge folded from [`PoolFact`] observations.
#[derive(Clone, Debug, Default)]
pub struct PoolGaugeModel {
    /// Every live lease, by `lease_id`: its consumer and bytes.
    leases: BTreeMap<u64, (Consumer, u64)>,
    /// Live leased bytes per consumer, summed from `leases`.
    by_consumer: [u64; Consumer::COUNT],
    /// Live leased bytes across every consumer.
    total_leased: u64,
    /// Peak leased bytes per consumer over the run.
    peaks_by_consumer: [u64; Consumer::COUNT],
    /// Peak total leased bytes over the run.
    peak_total: u64,
    /// Peak leased bytes per consumer at a growth event (a grant or a
    /// growing resize), where the pool reports the consumer's cap. A move
    /// ignores the target's cap, so drains it adds count only once a later
    /// growth event sees them.
    granted_peaks: [u64; Consumer::COUNT],
    /// The cap each consumer's granted peak was granted under.
    caps_at_peak: [Option<u64>; Consumer::COUNT],
    /// Configured `Sessions` floor bytes for `mem-13` floor preservation.
    sessions_floor: u64,
    /// Whether the node overrides the default session settings. Only with
    /// the defaults must `Sessions` stay within its protected share.
    session_overrides: bool,
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
    waited_reserved: Vec<(Consumer, u64, LeaseDenied)>,
    /// Number of `PoolFact::Granted` events recorded on reserved consumers (`mem-5`).
    reserved_grants_observed: u64,
    /// The height each labeled live lease funds, by `lease_id`. `None` marks
    /// a drain a move left behind (`mem-15`).
    heights: BTreeMap<u64, Option<u32>>,
}

/// Whether `consumer` holds a fixed reserve (`control`, `propagation`,
/// `consensus`).
fn is_reserved(consumer: Consumer) -> bool {
    consumer.priority() == Priority::Reserved
}

impl PoolGaugeModel {
    /// An empty gauge model configured with `sessions_floor` and whether the
    /// node uses default session settings.
    pub fn new(sessions_floor: u64, default_session_settings: bool) -> Self {
        Self {
            sessions_floor,
            session_overrides: !default_session_settings,
            ..Self::default()
        }
    }

    /// Fold one [`PoolFact`] into the gauge.
    pub fn observe(&mut self, fact: &PoolFact) {
        self.events_observed = self.events_observed.saturating_add(1);
        match *fact {
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
            } => {
                self.count_sessions_event(consumer);
                if is_reserved(consumer) {
                    self.reserved_grants_observed = self.reserved_grants_observed.saturating_add(1);
                }
                if self.leases.insert(lease_id, (consumer, bytes)).is_some() {
                    self.bound_violation(
                        "each granted lease carries a fresh lease_id",
                        format!(
                            "lease_id {lease_id} for consumer `{}` ({bytes} B) was already live",
                            consumer.label()
                        ),
                    );
                }
                self.add_bytes(consumer, bytes);
                if consumer == Consumer::Sessions {
                    if let Some(conn_id) = peer {
                        self.sessions_waiting_conns.remove(&conn_id);
                    }
                }
                self.check_reported_bounds(
                    consumer,
                    consumer_leased,
                    Some((consumer_cap, consumer_share, true)),
                    total_leased,
                    pool_limit,
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
                self.count_sessions_event(consumer);
                let label = consumer.label();
                match self.leases.get_mut(&lease_id) {
                    Some((live_consumer, live_bytes)) => {
                        let (live_consumer, held) = (*live_consumer, *live_bytes);
                        *live_bytes = new_bytes;
                        if live_consumer != consumer || held != old_bytes {
                            self.bound_violation(
                                format!(
                                    "resize of lease {lease_id} matches live consumer `{}` and {held} B",
                                    live_consumer.label()
                                ),
                                format!(
                                    "reported `{label}` resizing from {old_bytes} B to {new_bytes} B"
                                ),
                            );
                        }
                        if new_bytes >= old_bytes {
                            self.add_bytes(consumer, new_bytes - old_bytes);
                        } else {
                            self.sub_bytes(consumer, old_bytes - new_bytes);
                        }
                    }
                    None => {
                        self.bound_violation(
                            "only a live lease is resized",
                            format!(
                                "lease_id {lease_id} for `{label}` resized from {old_bytes} to {new_bytes} without being live"
                            ),
                        );
                    }
                }
                // `set_cap` may lower a cap below what a consumer holds, so
                // only a resize that grows the lease must fit the cap.
                self.check_reported_bounds(
                    consumer,
                    consumer_leased,
                    Some((consumer_cap, consumer_share, new_bytes > old_bytes)),
                    total_leased,
                    pool_limit,
                );
            }
            PoolFact::Split {
                from_lease_id,
                to_lease_id,
                consumer,
                bytes,
                ..
            } => {
                self.count_sessions_event(consumer);
                let label = consumer.label();
                match self.leases.get_mut(&from_lease_id) {
                    Some((live_consumer, live_bytes))
                        if *live_consumer == consumer && *live_bytes >= bytes =>
                    {
                        *live_bytes -= bytes;
                        if *live_bytes == 0 {
                            self.leases.remove(&from_lease_id);
                        }
                    }
                    other => {
                        let other = format!("source lease state was {other:?}");
                        self.bound_violation(
                            format!(
                                "split of {bytes} B from lease {from_lease_id} (`{label}`) takes from a live lease with at least {bytes} B"
                            ),
                            other,
                        );
                    }
                }
                if let Some(height) = self.heights.get(&from_lease_id).copied() {
                    self.heights.insert(to_lease_id, height);
                }
                if self.leases.insert(to_lease_id, (consumer, bytes)).is_some() {
                    self.bound_violation(
                        "split creates a child lease with a fresh lease_id",
                        format!("child lease_id {to_lease_id} for `{label}` was already live"),
                    );
                }
            }
            PoolFact::Merged {
                into_lease_id,
                from_lease_id,
                consumer,
                bytes,
                ..
            } => {
                self.count_sessions_event(consumer);
                let label = consumer.label();
                match self.leases.remove(&from_lease_id) {
                    Some((from_consumer, from_bytes))
                        if from_consumer == consumer && from_bytes == bytes => {}
                    other => {
                        self.bound_violation(
                            format!(
                                "merge consumes live lease {from_lease_id} (`{label}`, {bytes} B)"
                            ),
                            format!("removed source lease was {other:?}"),
                        );
                    }
                }
                if let Some(height) = self.heights.remove(&from_lease_id) {
                    self.heights.entry(into_lease_id).or_insert(height);
                }
                match self.leases.get_mut(&into_lease_id) {
                    Some((into_consumer, into_bytes)) if *into_consumer == consumer => {
                        *into_bytes = into_bytes.saturating_add(bytes);
                    }
                    other => {
                        let other = format!("target lease was {other:?}");
                        self.bound_violation(
                            format!("merge target lease {into_lease_id} (`{label}`) is live"),
                            other,
                        );
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
                match self.leases.get_mut(&from_lease_id) {
                    Some((live_consumer, live_bytes))
                        if *live_consumer == from && *live_bytes >= bytes =>
                    {
                        *live_bytes -= bytes;
                        if *live_bytes == 0 {
                            self.leases.remove(&from_lease_id);
                        }
                    }
                    other => {
                        let other = format!("source lease state was {other:?}");
                        self.bound_violation(
                            format!(
                                "move of {bytes} B from lease {from_lease_id} (`{}`) takes from a live lease with at least {bytes} B",
                                from.label()
                            ),
                            other,
                        );
                    }
                }
                if let Some(height) = self.heights.get(&from_lease_id).copied() {
                    self.heights.insert(to_lease_id, height);
                }
                if self.leases.insert(to_lease_id, (to, bytes)).is_some() {
                    self.bound_violation(
                        "a move creates a lease with a fresh lease_id",
                        format!(
                            "lease_id {to_lease_id} for `{}` was already live",
                            to.label()
                        ),
                    );
                }
                self.sub_bytes(from, bytes);
                self.add_bytes(to, bytes);
                // A move adds no ledger bytes, so it checks totals but not
                // the target's cap.
                self.check_reported_bounds(from, from_leased, None, total_leased, pool_limit);
                self.check_reported_bounds(to, to_leased, None, total_leased, pool_limit);
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
                self.count_sessions_event(consumer);
                self.heights.remove(&lease_id);
                let label = consumer.label();
                match self.leases.remove(&lease_id) {
                    Some((live_consumer, live_bytes)) => {
                        if live_consumer != consumer || live_bytes != bytes {
                            self.bound_violation(
                                format!(
                                    "released lease {lease_id} matches live consumer `{}` and {live_bytes} B",
                                    live_consumer.label()
                                ),
                                format!("released `{label}` with {bytes} B"),
                            );
                        }
                        self.sub_bytes(consumer, bytes);
                    }
                    None => {
                        self.bound_violation(
                            "only a live lease is released",
                            format!(
                                "lease_id {lease_id} for `{label}` ({bytes} B) released without being live"
                            ),
                        );
                    }
                }
                self.check_reported_bounds(
                    consumer,
                    consumer_leased,
                    None,
                    total_leased,
                    pool_limit,
                );
            }
            PoolFact::Labeled { lease_id, height } => {
                self.heights.insert(lease_id, height);
            }
            PoolFact::Waited {
                consumer,
                peer,
                requested_bytes,
                reason,
            } => {
                if is_reserved(consumer) {
                    self.waited_reserved
                        .push((consumer, requested_bytes, reason));
                }
                if consumer == Consumer::Sessions {
                    self.sessions_events_observed = self.sessions_events_observed.saturating_add(1);
                    if let Some(conn_id) = peer {
                        self.sessions_waiting_conns.insert(conn_id);
                        if self.admitted_session_conns.contains(&conn_id) {
                            self.session_violations.push((
                                "a connection waits for Sessions memory only before it admits a session".to_owned(),
                                format!(
                                    "connection {conn_id} admitted service sessions, then its Sessions admission waited ({reason:?})"
                                ),
                            ));
                        }
                    }
                    // Floor preservation: live sessions must never be blocked
                    // below floor by pool contention.
                    let current = self.by_consumer[Consumer::Sessions.index()];
                    if matches!(
                        reason,
                        LeaseDenied::PoolFull | LeaseDenied::BelowShareWaiting
                    ) && current.saturating_add(requested_bytes) <= self.sessions_floor
                    {
                        self.session_violations.push((
                            format!(
                                "Sessions leases up to floor ({} B) are never blocked by pool contention",
                                self.sessions_floor
                            ),
                            format!(
                                "Sessions waited with `{reason:?}` while requesting {requested_bytes} B at {current} B leased"
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

    fn count_sessions_event(&mut self, consumer: Consumer) {
        if consumer == Consumer::Sessions {
            self.sessions_events_observed = self.sessions_events_observed.saturating_add(1);
        }
    }

    fn bound_violation(&mut self, expected: impl Into<String>, actual: String) {
        self.bound_violations.push((expected.into(), actual));
    }

    fn add_bytes(&mut self, consumer: Consumer, bytes: u64) {
        let index = consumer.index();
        self.by_consumer[index] = self.by_consumer[index].saturating_add(bytes);
        self.peaks_by_consumer[index] = self.peaks_by_consumer[index].max(self.by_consumer[index]);
        self.total_leased = self.total_leased.saturating_add(bytes);
        self.peak_total = self.peak_total.max(self.total_leased);
    }

    fn sub_bytes(&mut self, consumer: Consumer, bytes: u64) {
        let held = self.by_consumer[consumer.index()];
        if held < bytes {
            self.bound_violation(
                format!(
                    "consumer `{}` never releases more bytes than it holds",
                    consumer.label()
                ),
                format!("held {held} B, released {bytes} B"),
            );
        }
        self.by_consumer[consumer.index()] = held.saturating_sub(bytes);
        if self.total_leased < bytes {
            self.bound_violation(
                "pool total never underflows on release",
                format!("total {} B, released {bytes} B", self.total_leased),
            );
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
        consumer: Consumer,
        reported_consumer: u64,
        limits: Option<(u64, u64, bool)>,
        reported_total: u64,
        pool_limit: u64,
    ) {
        let label = consumer.label();
        let folded_consumer = self.by_consumer[consumer.index()];
        if folded_consumer != reported_consumer {
            self.bound_violation(
                format!(
                    "folded live leases for `{label}` ({folded_consumer} B) match reported consumer total"
                ),
                format!("pool reported {reported_consumer} B"),
            );
        }
        if self.total_leased != reported_total {
            self.bound_violation(
                format!(
                    "folded pool total ({} B) matches reported total",
                    self.total_leased
                ),
                format!("pool reported {reported_total} B"),
            );
        }
        if reported_total > pool_limit || self.total_leased > pool_limit {
            self.bound_violation(
                format!("live pool leases stay within S + floors ({pool_limit} B)"),
                format!(
                    "reported total {reported_total} B (folded {} B)",
                    self.total_leased
                ),
            );
        }
        let Some((cap, share, true)) = limits else {
            return;
        };
        if folded_consumer >= self.granted_peaks[consumer.index()] {
            self.granted_peaks[consumer.index()] = folded_consumer;
            self.caps_at_peak[consumer.index()] = Some(cap);
        }
        if reported_consumer > cap || folded_consumer > cap {
            self.bound_violation(
                format!("consumer `{label}` stays within its cap ({cap} B)"),
                format!("reported {reported_consumer} B (folded {folded_consumer} B)"),
            );
        }
        if consumer == Consumer::Sessions
            && !self.session_overrides
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

    /// Combined live leased bytes for `BlockSync` + `Consensus` (block sync's
    /// requests and retained bodies across commit-window and look-ahead).
    pub fn block_sync_total_leased(&self) -> u64 {
        self.by_consumer[Consumer::BlockSync.index()]
            .saturating_add(self.by_consumer[Consumer::Consensus.index()])
    }

    /// Total live leased bytes across every consumer.
    pub fn total_leased(&self) -> u64 {
        self.total_leased
    }

    /// Number of currently live leases tracked by ID.
    pub fn live_lease_count(&self) -> usize {
        self.leases.len()
    }

    /// Peak leased bytes recorded per consumer, by [`Consumer::index`].
    pub fn peaks_by_consumer(&self) -> [u64; Consumer::COUNT] {
        self.peaks_by_consumer
    }

    /// Each consumer's peak at a growth event, by [`Consumer::index`].
    pub fn granted_peaks(&self) -> [u64; Consumer::COUNT] {
        self.granted_peaks
    }

    /// The cap each consumer's granted peak was granted under, by
    /// [`Consumer::index`].
    pub fn caps_at_peak(&self) -> [Option<u64>; Consumer::COUNT] {
        self.caps_at_peak
    }

    /// G1 (`memory/pool-bounded`) violations folded so far.
    pub fn bound_violations(&self) -> &[(String, String)] {
        &self.bound_violations
    }

    /// Reserved-consumer waits folded so far (`(consumer, requested_bytes, reason)`).
    pub fn waited_reserved(&self) -> &[(Consumer, u64, LeaseDenied)] {
        &self.waited_reserved
    }

    /// Number of reserved-consumer grants folded so far (`mem-5`).
    pub fn reserved_grants_observed(&self) -> u64 {
        self.reserved_grants_observed
    }

    /// The heights the live `Consensus` leases fund, one entry per lease
    /// that funds a height, and the first live `Consensus` lease that names
    /// no height. A drain a move left behind funds none (`mem-15`).
    pub fn consensus_heights(&self) -> (Vec<(u32, u64)>, Option<u64>) {
        let mut heights = Vec::new();
        let mut unlabeled = None;
        for (lease_id, (consumer, bytes)) in &self.leases {
            if *consumer != Consumer::Consensus || *bytes == 0 {
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

/// [`TIP_NEVER_WAITS`] (`mem-5`): reserved tip-priority consumers (`control`,
/// `propagation`, `consensus`) never wait on the memory pool, even while bulk
/// consumers saturate the shared space.
pub fn tip_never_waits<S: HasView>() -> Property<S>
where
    S::View: PoolView,
{
    Property {
        id: PropertyId::new(TIP_NEVER_WAITS),
        claim: "reserved tip-priority consumers (control, propagation, consensus) reach their \
                bounds without waiting on the memory pool, even when bulk consumers saturate",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: Status::Active,
        check: check_tip_never_waits::<S>,
        witness: Some(TriggerHistory::Log(reserved_lease_observed)),
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
            } if *consumer == Consumer::Consensus => {
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

/// Fail with the first of `violations`, recording the rest, or hold when
/// there are none.
fn report_violations<S: HasView>(
    cx: &mut CheckCtx<'_, S>,
    violations: Vec<(String, String)>,
) -> Verdict {
    let mut violations = violations.into_iter();
    let Some((expected, actual)) = violations.next() else {
        return Verdict::Holds;
    };
    for (expected, actual) in violations {
        cx.record_violation(expected, actual);
    }
    cx.violated(expected, actual)
}

fn check_pool_bounded<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S::View: PoolView,
{
    let (observed, peak_total, peaks_by_consumer, violations) = cx.view().read_pool(|pool| {
        (
            pool.events_observed,
            pool.peak_total,
            pool.peaks_by_consumer,
            pool.bound_violations.clone(),
        )
    });
    if observed == 0 {
        return Verdict::NotApplicable;
    }
    cx.peak("pool-leased-bytes", peak_total);
    for (consumer, key) in [
        (Consumer::BlockSync, "block-sync-leased-bytes"),
        (Consumer::Consensus, "consensus-leased-bytes"),
        (Consumer::Serving, "serving-leased-bytes"),
        (Consumer::Sessions, "sessions-leased-bytes"),
        (Consumer::Tx, "tx-leased-bytes"),
    ] {
        let peak = peaks_by_consumer[consumer.index()];
        if peak > 0 {
            cx.peak(key, peak);
        }
    }
    report_violations(cx, violations)
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
        let nonzero: Vec<_> = Consumer::ALL
            .into_iter()
            .map(|consumer| (consumer, pool.by_consumer[consumer.index()]))
            .filter(|(_, bytes)| *bytes > 0)
            .collect();
        (pool.total_leased, pool.leases.len(), nonzero)
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
            pool.sessions_events_observed,
            pool.peaks_by_consumer[Consumer::Sessions.index()],
            pool.session_violations.clone(),
        )
    });
    if observed == 0 {
        return Verdict::NotApplicable;
    }
    cx.peak("sessions-leased-bytes", peak_sessions);
    report_violations(cx, violations)
}

fn check_tip_never_waits<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S::View: PoolView,
{
    let (reserved_grants, waited) = cx
        .view()
        .read_pool(|pool| (pool.reserved_grants_observed, pool.waited_reserved.clone()));
    if reserved_grants == 0 && waited.is_empty() {
        return Verdict::NotApplicable;
    }
    let violations = waited
        .into_iter()
        .map(|(consumer, bytes, reason)| {
            let consumer = consumer.label();
            (
                format!("reserved consumer `{consumer}` never waits on the memory pool"),
                format!("`{consumer}` waited for {bytes} B ({reason:?})"),
            )
        })
        .collect();
    report_violations(cx, violations)
}

/// [`TIP_NEVER_WAITS`]'s trigger history: the pool recorded at least one grant
/// or wait on a reserved consumer (`control`, `propagation`, `consensus`).
pub fn reserved_lease_observed(log: &ObservationLog) -> bool {
    log.entries().iter().any(|entry| {
        matches!(
            &entry.obs,
            Observation::Pool {
                event: PoolFact::Granted { consumer, .. } | PoolFact::Waited { consumer, .. },
                ..
            } if is_reserved(*consumer)
        )
    })
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
            } if *consumer == Consumer::Sessions
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A grant of `bytes` to `consumer` as lease `lease_id`.
    fn granted(lease_id: u64, consumer: Consumer, bytes: u64) -> PoolFact {
        PoolFact::Granted {
            lease_id,
            consumer,
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
        pool.observe(&granted(1, Consumer::Consensus, 10));
        assert_eq!(pool.consensus_heights(), (Vec::new(), Some(1)));
        pool.observe(&labeled(1, Some(7)));
        pool.observe(&granted(2, Consumer::Consensus, 10));
        pool.observe(&labeled(2, Some(7)));
        assert_eq!(pool.consensus_heights(), (vec![(7, 1), (7, 2)], None));
        pool.observe(&labeled(2, None));
        assert_eq!(pool.consensus_heights(), (vec![(7, 1)], None));
        pool.observe(&granted(3, Consumer::BlockSync, 10));
        pool.observe(&labeled(3, Some(7)));
        assert_eq!(pool.consensus_heights(), (vec![(7, 1)], None));
    }

    /// A split child keeps its parent's height, so a drain split from a
    /// commit-window lease and never moved shows as a second lease on it.
    #[test]
    fn a_split_drain_keeps_its_height_until_it_moves() {
        let mut pool = PoolGaugeModel::default();
        pool.observe(&granted(1, Consumer::Consensus, 10));
        pool.observe(&labeled(1, Some(7)));
        pool.observe(&PoolFact::Split {
            from_lease_id: 1,
            to_lease_id: 2,
            consumer: Consumer::Consensus,
            peer: Some(1),
            bytes: 4,
        });
        assert_eq!(pool.consensus_heights(), (vec![(7, 1), (7, 2)], None));
        pool.observe(&PoolFact::Moved {
            from_lease_id: 2,
            to_lease_id: 3,
            from: Consumer::Consensus,
            to: Consumer::BlockSync,
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
