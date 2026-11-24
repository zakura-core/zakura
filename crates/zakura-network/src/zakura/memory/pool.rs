//! Pure memory pool machine, shared pool rule, async [`MemoryPool`], and RAII
//! [`PoolLease`].
//!
//! [`MemoryPoolMachine`] and [`super::peer_pool::PeerSubPool`] own every decision: grant,
//! wait or refuse, and whose waiters a change wakes. [`MemoryPool`] holds both
//! machines under one lock, applies their effects after unlocking (wakeups,
//! metrics, the overrun warning), and owns the one async wait loop.

use std::{fmt, future::Future, sync::Arc};

#[cfg(any(test, zakura_sim))]
use zakura_rt::sync::counter::AtomicU64 as CounterU64;
use zakura_rt::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, MutexGuard, Notify, PoisonError,
    },
    time::Instant,
};

use super::{
    peer_pool::{serving_ceiling, try_serving_lease, Level, PeerKey, PeerSubPool},
    Consumer, PoolConfig, Priority, OVERRUN_WARN_INTERVAL,
};
use crate::zakura::{ZakuraConnId, ZakuraPeerId};

/// Why a lease request was refused by the pool rule (`02 §5`).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LeaseDenied {
    /// Taking the lease would exceed the consumer's cap (rule 1).
    CapExceeded,
    /// The shared space `S` does not have enough free bytes above the
    /// consumer's floor (rule 2).
    PoolFull,
    /// The consumer is at or above its share while another consumer below its
    /// share waits (rule 3).
    BelowShareWaiting,
    /// A non-borrowing consumer (`Sessions`) would exceed its protected share
    /// (rule 4).
    ShareExceeded,
}

/// A refused lease request, and whether waiting can satisfy it.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Refusal {
    /// Pool contention (rules 2 and 3): another consumer's release can satisfy
    /// the request. A caller that waits for it marks its consumer waiting.
    Contended(LeaseDenied),
    /// The requester's own leases fill its cap or share: only its own
    /// releases can satisfy the request. Waiting for them marks nothing.
    OwnLimit(LeaseDenied),
    /// Waiting can never satisfy the request: it exceeds what the requester
    /// can ever hold, or the requester's leases are long-lived (rule 4).
    Final(LeaseDenied),
}

impl Refusal {
    /// The pool rule that refused the request.
    pub fn reason(self) -> LeaseDenied {
        match self {
            Self::Contended(reason) | Self::OwnLimit(reason) | Self::Final(reason) => reason,
        }
    }
}

/// Inputs to the pool rule (`02 §5`), shared by the node level
/// ([`MemoryPoolMachine`]) and serving's peer level ([`super::peer_pool::PeerSubPool`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PoolRuleInput {
    /// Bytes the requester holds.
    pub(crate) leased: u64,
    /// Additional bytes requested (`> 0`).
    pub(crate) request: u64,
    /// The requester's floor.
    pub(crate) floor: u64,
    /// The requester's cap (`>= floor`).
    pub(crate) cap: u64,
    /// The requester's share of `S`, above its floor.
    pub(crate) share_of_s: u64,
    /// Free bytes in `S`, or `None` at a level whose parent checks room.
    pub(crate) shared_free: Option<u64>,
    /// Most bytes the requester can ever hold.
    pub(crate) reachable: u64,
    /// Whether the requester may borrow above its share.
    pub(crate) can_borrow: bool,
    /// Whether the requester's leases are long-lived, so it never waits for
    /// its own releases.
    pub(crate) long_lived: bool,
    /// Whether another requester below its share waits on contention.
    pub(crate) other_below_share_waiting: bool,
}

/// Evaluates the pool rule (`02 §5`) for a non-empty request:
///
/// 1. The requester stays within its cap.
/// 2. Its floor still has room, or `S` has the bytes the floor doesn't cover.
/// 3. It is below its share, or no other requester below its share waits.
/// 4. A requester that can't borrow stays within its share.
///
/// Rule 3 binds only a requester at or above its share, so two requesters
/// below their shares never refuse each other.
///
/// A grant returns the bytes it draws from `S` (0 when the floor covers the
/// request).
#[inline]
pub(crate) fn check_pool_rule(input: PoolRuleInput) -> Result<u64, Refusal> {
    let protected = input.floor.saturating_add(input.share_of_s);
    let own_limit = |reason| {
        if input.long_lived {
            Refusal::Final(reason)
        } else {
            Refusal::OwnLimit(reason)
        }
    };

    if input.request > input.cap {
        return Err(Refusal::Final(LeaseDenied::CapExceeded));
    }
    if input.request > input.reachable {
        return Err(Refusal::Final(if input.can_borrow {
            LeaseDenied::PoolFull
        } else {
            LeaseDenied::ShareExceeded
        }));
    }
    let Some(next_leased) = input.leased.checked_add(input.request) else {
        return Err(Refusal::Final(LeaseDenied::CapExceeded));
    };

    // Rule 1.
    if next_leased > input.cap {
        return Err(own_limit(LeaseDenied::CapExceeded));
    }
    // Rule 4.
    if !input.can_borrow && next_leased > protected {
        return Err(own_limit(LeaseDenied::ShareExceeded));
    }
    // Rule 3.
    if input.leased >= protected && input.other_below_share_waiting {
        return Err(Refusal::Contended(LeaseDenied::BelowShareWaiting));
    }
    // Rule 2.
    let need_from_shared = next_leased.saturating_sub(input.floor.max(input.leased));
    if let Some(shared_free) = input.shared_free {
        if need_from_shared > shared_free {
            return Err(Refusal::Contended(LeaseDenied::PoolFull));
        }
    }

    Ok(need_from_shared)
}

/// Report returned by [`MemoryPoolMachine::check_overrun`] when live memory
/// exceeds `B` and the 10-minute warning interval has elapsed (`02 §8`, mem-14).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OverrunReport {
    /// Current live total (`fixed_bytes + total_leased_bytes`).
    pub live_bytes: u64,
    /// Configured P2P budget `B`.
    pub budget_bytes: u64,
    /// `live_bytes - budget_bytes`.
    pub excess_bytes: u64,
}

/// Snapshot of one consumer's live accounting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerSnapshot {
    /// Which consumer this snapshot describes.
    pub consumer: Consumer,
    /// Bytes currently leased.
    pub leased: u64,
    /// Reserved floor.
    pub floor: u64,
    /// Effective cap (`>= floor`).
    pub cap: u64,
    /// Weight in `S` (0 for reserved consumers).
    pub weight: u64,
    /// Protected total (`floor + share_of_S`).
    pub protected_share: u64,
    /// Whether a caller waits on pool contention for this consumer.
    pub waiting: bool,
}

/// Snapshot of the entire pool's live accounting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolSnapshot {
    /// Total P2P budget `B`.
    pub budget_b: u64,
    /// Fixed non-leased bytes.
    pub fixed_bytes: u64,
    /// Total shared capacity `S`.
    pub shared_capacity: u64,
    /// Bytes currently drawn from `S`.
    pub shared_used: u64,
    /// Total live leased bytes across all consumers.
    pub total_leased: u64,
    /// Hard lease ceiling (`total_floors + shared_capacity`).
    pub lease_limit: u64,
    /// Per-consumer snapshots in canonical order.
    pub consumers: [ConsumerSnapshot; Consumer::COUNT],
}

/// Which of a consumer's two waiter queues a waiter joins. Below-share
/// waiters wake first (`02 §5`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WakeClass {
    /// The waiter's consumer, or serving peer, is below its share.
    Below,
    /// The waiter's consumer, or serving peer, is at or above its share.
    Above,
}

impl WakeClass {
    /// Both classes, in wake order.
    pub const ALL: [Self; 2] = [Self::Below, Self::Above];

    /// The class of a requester that is (or isn't) below its share.
    pub fn of(below_share: bool) -> Self {
        if below_share {
            Self::Below
        } else {
            Self::Above
        }
    }

    /// Fixed array index for this class.
    #[inline]
    pub const fn index(self) -> usize {
        match self {
            Self::Below => 0,
            Self::Above => 1,
        }
    }
}

/// Internal per-consumer runtime state stored inline in `[ConsumerState; 7]`.
/// The floor, weight and planned cap stay in the machine's [`PoolConfig`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ConsumerState {
    /// The dynamic cap, between the floor and the plan's cap.
    cap: u64,
    leased: u64,
    /// Waiters on pool contention, wait tokens included. Rule 3 counts them.
    contended: u32,
    /// Async waiters per [`WakeClass`].
    waiters: [u32; 2],
    /// Async waiters per [`WakeClass`] still due a wakeup in the current
    /// round, so every waiter tries once per change.
    turns: [u32; 2],
    /// Wait tokens held by callers that poll [`MemoryPool::notifier`].
    tokens: u32,
    /// A refused `try_lease` armed one broadcast on [`MemoryPool::notifier`].
    poll_armed: bool,
}

/// Bitmask of consumers (`1 << consumer.index()`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ConsumerMask(u8);

impl ConsumerMask {
    #[inline]
    fn set(&mut self, consumer: Consumer) {
        self.0 |= 1u8 << consumer.index();
    }

    #[inline]
    fn contains(self, consumer: Consumer) -> bool {
        (self.0 & (1u8 << consumer.index())) != 0
    }
}

/// Wakeups a pool change calls for, applied by [`MemoryPool`] after unlocking.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Wake {
    /// Consumers whose next waiter in each [`WakeClass`] wakes.
    waiters: [ConsumerMask; 2],
    /// Consumers whose [`MemoryPool::notifier`] pollers all wake.
    pollers: ConsumerMask,
}

impl Wake {
    /// Whether this wakes nobody.
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// Whether this wakes `consumer`'s next waiter in `class`.
    pub fn wakes_waiter(&self, consumer: Consumer, class: WakeClass) -> bool {
        self.waiters[class.index()].contains(consumer)
    }

    /// Whether this wakes `consumer`'s pollers.
    pub fn wakes_pollers(&self, consumer: Consumer) -> bool {
        self.pollers.contains(consumer)
    }

    fn merge(&mut self, other: Self) {
        self.waiters[0].0 |= other.waiters[0].0;
        self.waiters[1].0 |= other.waiters[1].0;
        self.pollers.0 |= other.pollers.0;
    }
}

/// What [`MemoryPoolMachine::release`] returned to the pool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Released {
    /// Bytes released (clamped to the consumer's leased bytes).
    pub bytes: u64,
    /// Bytes returned to `S`.
    pub freed_shared: u64,
}

/// Pure, allocation-free state machine for the node-wide memory pool (`02 §5`).
///
/// It decides grants, waits and refusals, and which waiters a change wakes.
/// Proptests and unit tests drive it directly without any runtime, lock, or
/// secondary model (decision-15).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryPoolMachine {
    /// The plan, normalized as [`PoolConfig::with_consumer`] does: a reserved
    /// consumer's cap is its floor and it has no weight, and a bulk cap is at
    /// least its floor. Each spec's cap is the plan's cap, which
    /// [`Self::set_cap`] never raises past.
    config: PoolConfig,
    shared_capacity: u64,
    shared_used: u64,
    total_leased: u64,
    total_floors: u64,
    total_weight: u128,
    consumers: [ConsumerState; Consumer::COUNT],
    last_overrun_warn: Option<Instant>,
}

impl MemoryPoolMachine {
    /// Construct a new pool machine from `config`.
    pub fn new(config: PoolConfig) -> Self {
        let config = Consumer::ALL.into_iter().fold(config, |config, consumer| {
            config.with_consumer(consumer, config.consumers[consumer.index()])
        });
        let total_weight = config
            .consumers
            .iter()
            .map(|spec| u128::from(spec.weight))
            .sum();
        Self {
            shared_capacity: config.shared_capacity(),
            shared_used: 0,
            total_leased: 0,
            total_floors: config.total_floors(),
            total_weight,
            consumers: Consumer::ALL.map(|consumer| ConsumerState {
                cap: config.consumers[consumer.index()].cap,
                ..ConsumerState::default()
            }),
            config,
            last_overrun_warn: None,
        }
    }

    /// Total P2P budget `B`.
    #[inline]
    pub fn budget_b(&self) -> u64 {
        self.config.budget_b
    }

    /// Non-leased fixed bytes.
    #[inline]
    pub fn fixed_bytes(&self) -> u64 {
        self.config.fixed_bytes
    }

    /// Total shared capacity `S = B - fixed - reserves - Σ bulk_floors`
    /// (saturating at 0 under operator overrides).
    #[inline]
    pub fn shared_capacity(&self) -> u64 {
        self.shared_capacity
    }

    /// Bytes currently drawn from `S`.
    #[inline]
    pub fn shared_used(&self) -> u64 {
        self.shared_used
    }

    /// Free bytes remaining in `S`.
    #[inline]
    pub fn shared_free(&self) -> u64 {
        self.shared_capacity.saturating_sub(self.shared_used)
    }

    /// Sum of live leases across all consumers.
    #[inline]
    pub fn total_leased(&self) -> u64 {
        self.total_leased
    }

    /// Hard ceiling on total leased bytes (`total_floors + shared_capacity`).
    #[inline]
    pub fn lease_limit(&self) -> u64 {
        self.total_floors.saturating_add(self.shared_capacity)
    }

    /// Bytes currently leased by `consumer`.
    #[inline]
    pub fn leased(&self, consumer: Consumer) -> u64 {
        self.consumers[consumer.index()].leased
    }

    /// Reserved floor of `consumer`.
    #[inline]
    pub fn floor(&self, consumer: Consumer) -> u64 {
        self.config.consumers[consumer.index()].floor
    }

    /// Effective cap of `consumer` (`>= floor`).
    #[inline]
    pub fn cap(&self, consumer: Consumer) -> u64 {
        self.consumers[consumer.index()].cap
    }

    /// The plan's cap for `consumer`, which bounds [`Self::set_cap`].
    #[inline]
    pub fn planned_cap(&self, consumer: Consumer) -> u64 {
        self.config.consumers[consumer.index()].cap
    }

    /// Bytes `consumer` holds above its floor, which it draws from `S`.
    #[inline]
    fn shared_bytes(&self, consumer: Consumer) -> u64 {
        self.leased(consumer).saturating_sub(self.floor(consumer))
    }

    /// Consumer's weighted share of `S` (bytes above its floor).
    #[inline]
    pub fn share_of_s(&self, consumer: Consumer) -> u64 {
        if self.total_weight == 0 {
            return 0;
        }
        let weight = u128::from(self.config.consumers[consumer.index()].weight);
        let share = u128::from(self.shared_capacity).saturating_mul(weight) / self.total_weight;
        // `share <= self.shared_capacity <= u64::MAX` always holds.
        u64::try_from(share).unwrap_or(u64::MAX)
    }

    /// Consumer's protected total (`floor + share_of_S`).
    #[inline]
    pub fn protected_share(&self, consumer: Consumer) -> u64 {
        self.floor(consumer)
            .saturating_add(self.share_of_s(consumer))
    }

    /// Whether `consumer` is below both its protected share and its cap.
    #[inline]
    pub fn is_below_share(&self, consumer: Consumer) -> bool {
        let state = &self.consumers[consumer.index()];
        state.leased < self.protected_share(consumer).min(state.cap)
    }

    /// Whether a caller waits on pool contention for `consumer`.
    #[inline]
    pub fn is_waiting(&self, consumer: Consumer) -> bool {
        self.consumers[consumer.index()].contended > 0
    }

    /// Async waiters registered on `consumer` in `class`.
    #[inline]
    pub fn waiters(&self, consumer: Consumer, class: WakeClass) -> u32 {
        self.consumers[consumer.index()].waiters[class.index()]
    }

    /// Whether a consumer other than `skip` waits on contention below its
    /// share. Such a waiter holds every other consumer at or above its share
    /// to it (rule 3).
    #[inline]
    pub fn any_below_share_waiting_except(&self, skip: Option<Consumer>) -> bool {
        Consumer::BULK.iter().any(|&consumer| {
            Some(consumer) != skip && self.is_waiting(consumer) && self.is_below_share(consumer)
        })
    }

    /// Set the dynamic cap of a bulk consumer, clamped between its floor and
    /// the plan's cap.
    ///
    /// Reserved consumers have fixed proof-of-work/layout bounds (`floor == cap`)
    /// so calls for reserved consumers are ignored.
    pub fn set_cap(&mut self, consumer: Consumer, new_cap: u64) {
        if consumer.priority() == Priority::Reserved {
            return;
        }
        self.consumers[consumer.index()].cap =
            new_cap.clamp(self.floor(consumer), self.planned_cap(consumer));
    }

    /// Decide a lease of `bytes` for `consumer` under the pool rule (`02 §5`),
    /// and take it when granted.
    ///
    /// A zero-byte request is always granted. A refusal changes no rule
    /// state: only a registered waiter marks its consumer waiting. A refusal
    /// that waiting can satisfy arms one broadcast on
    /// [`MemoryPool::notifier`], for callers that poll.
    pub fn try_lease(&mut self, consumer: Consumer, bytes: u64) -> Result<(), Refusal> {
        if bytes == 0 {
            return Ok(());
        }
        let state = self.consumers[consumer.index()];
        let floor = self.floor(consumer);
        let protected = self.protected_share(consumer);
        let reachable = if consumer.can_borrow() {
            state.cap.min(floor.saturating_add(self.shared_capacity))
        } else {
            state.cap.min(protected)
        };
        let input = PoolRuleInput {
            leased: state.leased,
            request: bytes,
            floor,
            cap: state.cap,
            share_of_s: protected - floor,
            shared_free: Some(self.shared_free()),
            reachable,
            can_borrow: consumer.can_borrow(),
            long_lived: consumer.long_lived(),
            other_below_share_waiting: self.any_below_share_waiting_except(Some(consumer)),
        };

        match check_pool_rule(input) {
            Ok(need_from_shared) => {
                self.consumers[consumer.index()].leased += bytes;
                self.shared_used += need_from_shared;
                self.total_leased += bytes;
                debug_assert!(self.shared_used <= self.shared_capacity);
                debug_assert!(self.total_leased <= self.lease_limit());
                Ok(())
            }
            Err(refusal) => {
                if !matches!(refusal, Refusal::Final(_)) {
                    self.consumers[consumer.index()].poll_armed = true;
                }
                Err(refusal)
            }
        }
    }

    /// Release `bytes` previously leased by `consumer`.
    ///
    /// Clamps to `leased` in release builds and debug-asserts `bytes <= leased`.
    pub fn release(&mut self, consumer: Consumer, bytes: u64) -> Released {
        let old_shared = self.shared_bytes(consumer);
        let state = &mut self.consumers[consumer.index()];
        debug_assert!(
            bytes <= state.leased,
            "pool release underflow for {:?}: releasing {bytes} > leased {}",
            consumer,
            state.leased
        );
        let actual = bytes.min(state.leased);
        state.leased -= actual;
        let freed_shared = old_shared - self.shared_bytes(consumer);
        self.shared_used -= freed_shared;
        self.total_leased -= actual;
        Released {
            bytes: actual,
            freed_shared,
        }
    }

    /// Move up to `bytes` of `from`'s leased bytes to `to`, and return the
    /// bytes moved.
    ///
    /// A move adds no ledger bytes, so it ignores `to`'s cap and share. It
    /// fills `to`'s unleased floor first and then free `S`; a reserved `to`
    /// takes only its floor. The ledger stays within the lease limit.
    pub fn transfer(&mut self, from: Consumer, to: Consumer, bytes: u64) -> u64 {
        if from == to {
            return 0;
        }
        let floor_room = self.floor(to).saturating_sub(self.leased(to));
        // A bulk `to` takes free `S` plus the `S` bytes `from` returns.
        let room = match to.priority() {
            Priority::Reserved => floor_room,
            Priority::Bulk => floor_room
                .saturating_add(self.shared_free())
                .saturating_add(self.shared_bytes(from)),
        };
        let moved = bytes.min(self.leased(from)).min(room);
        if moved == 0 {
            return 0;
        }
        let released = self.release(from, moved);
        let old_shared = self.shared_bytes(to);
        self.consumers[to.index()].leased += released.bytes;
        let taken_shared = self.shared_bytes(to) - old_shared;
        self.shared_used += taken_shared;
        self.total_leased += released.bytes;
        debug_assert!(self.shared_used <= self.shared_capacity);
        debug_assert!(self.total_leased <= self.lease_limit());
        released.bytes
    }

    /// Register one async waiter on `consumer` in `class`; `contended` says
    /// whether it waits on pool contention.
    pub fn add_waiter(&mut self, consumer: Consumer, class: WakeClass, contended: bool) {
        let state = &mut self.consumers[consumer.index()];
        state.waiters[class.index()] += 1;
        state.contended += u32::from(contended);
    }

    /// Unregister one waiter added by [`Self::add_waiter`].
    pub fn remove_waiter(&mut self, consumer: Consumer, class: WakeClass, contended: bool) {
        let state = &mut self.consumers[consumer.index()];
        let class = class.index();
        state.waiters[class] -= 1;
        state.turns[class] = state.turns[class].min(state.waiters[class]);
        state.contended -= u32::from(contended);
    }

    /// Register one wait token: a caller that polls [`MemoryPool::notifier`]
    /// and waits on pool contention.
    pub fn add_token(&mut self, consumer: Consumer) {
        let state = &mut self.consumers[consumer.index()];
        state.tokens += 1;
        state.contended += 1;
    }

    /// Unregister one wait token added by [`Self::add_token`].
    pub fn remove_token(&mut self, consumer: Consumer) {
        let state = &mut self.consumers[consumer.index()];
        state.tokens -= 1;
        state.contended -= 1;
    }

    /// Whose waiters to wake after a change (`02 §5`).
    ///
    /// - `released_by` returned bytes or gained room: its own waiters may fit.
    /// - When bytes returned to `S`, or rule 3 stopped holding consumers back
    ///   (`rule3_before` says whether it held before the change), other
    ///   consumers' waiters may fit. While a consumer below its share waits on
    ///   contention, only consumers below their shares wake.
    ///
    /// Each woken consumer starts a round: one async waiter wakes now, below
    /// its share first, and each woken waiter passes the next turn on
    /// ([`Self::take_turn`]) until every waiter tried once. Every poller of a
    /// woken consumer wakes once.
    pub fn wake(
        &mut self,
        released_by: Option<Consumer>,
        freed_shared: bool,
        rule3_before: bool,
    ) -> Wake {
        let mut wake = Wake::default();
        if let Some(consumer) = released_by {
            self.wake_consumer(consumer, &mut wake);
        }
        let rule3_now = self.any_below_share_waiting_except(None);
        if freed_shared || (rule3_before && !rule3_now) {
            for consumer in Consumer::BULK {
                if Some(consumer) != released_by && (!rule3_now || self.is_below_share(consumer)) {
                    self.wake_consumer(consumer, &mut wake);
                }
            }
        }
        wake
    }

    /// Wake `consumer`'s next waiter due a turn in the current round. A woken
    /// waiter calls this once it has tried, whatever the outcome.
    pub fn take_turn(&mut self, consumer: Consumer) -> Wake {
        let mut wake = Wake::default();
        let state = &mut self.consumers[consumer.index()];
        if let Some(class) = WakeClass::ALL
            .into_iter()
            .find(|class| state.turns[class.index()] > 0)
        {
            state.turns[class.index()] -= 1;
            wake.waiters[class.index()].set(consumer);
        }
        wake
    }

    fn wake_consumer(&mut self, consumer: Consumer, wake: &mut Wake) {
        let state = &mut self.consumers[consumer.index()];
        state.turns = state.waiters;
        if state.poll_armed || state.tokens > 0 {
            state.poll_armed = false;
            wake.pollers.set(consumer);
        }
        wake.merge(self.take_turn(consumer));
    }

    /// Current total budget available to `consumer` right now (used to size
    /// serving's [`super::peer_pool::PeerSubPool`]).
    ///
    /// If another consumer below its share is waiting, caps at `protected_share`;
    /// otherwise includes `shared_free`, bounded by `cap`.
    pub fn available_ceiling(&self, consumer: Consumer) -> u64 {
        let cap = self.cap(consumer);
        if !consumer.can_borrow() || self.any_below_share_waiting_except(Some(consumer)) {
            self.protected_share(consumer).min(cap)
        } else {
            self.floor(consumer)
                .saturating_add(self.shared_bytes(consumer))
                .saturating_add(self.shared_free())
                .min(cap)
        }
    }

    /// Whether live memory (`fixed_bytes + total_leased`) exceeds `B`, which
    /// only operator overrides allow.
    #[inline]
    pub fn is_over_budget(&self) -> bool {
        self.fixed_bytes().saturating_add(self.total_leased) > self.budget_b()
    }

    /// Check whether live memory (`fixed_bytes + total_leased`) exceeds `B`.
    ///
    /// Rate-limited to at most one [`OverrunReport`] per
    /// [`OVERRUN_WARN_INTERVAL`] (10 minutes, `02 §8`, mem-14). Under default
    /// settings where `fixed + reserves + Σ floors <= B`, live memory never
    /// exceeds `B`, so this always returns `None`.
    pub fn check_overrun(&mut self, now: Instant) -> Option<OverrunReport> {
        let budget_bytes = self.budget_b();
        let live_bytes = self.fixed_bytes().saturating_add(self.total_leased);
        if live_bytes <= budget_bytes {
            return None;
        }
        if let Some(last) = self.last_overrun_warn {
            if now.saturating_duration_since(last) < OVERRUN_WARN_INTERVAL {
                return None;
            }
        }
        self.last_overrun_warn = Some(now);
        Some(OverrunReport {
            live_bytes,
            budget_bytes,
            excess_bytes: live_bytes - budget_bytes,
        })
    }

    /// Take an immutable snapshot of all counters and consumer states.
    pub fn snapshot(&self) -> PoolSnapshot {
        PoolSnapshot {
            budget_b: self.budget_b(),
            fixed_bytes: self.fixed_bytes(),
            shared_capacity: self.shared_capacity,
            shared_used: self.shared_used,
            total_leased: self.total_leased,
            lease_limit: self.lease_limit(),
            consumers: Consumer::ALL.map(|consumer| ConsumerSnapshot {
                consumer,
                leased: self.leased(consumer),
                floor: self.floor(consumer),
                cap: self.cap(consumer),
                weight: self.config.consumers[consumer.index()].weight,
                protected_share: self.protected_share(consumer),
                waiting: self.is_waiting(consumer),
            }),
        }
    }
}

/// One pool lease event reported to a test or simulator [`PoolObserver`]
/// (`03 §4`).
#[cfg(any(test, zakura_sim))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolEvent {
    /// A new lease was granted.
    Granted {
        /// Unique monotonic lease identifier within this pool.
        lease_id: u64,
        /// Consumer owning the lease.
        consumer: Consumer,
        /// Peer connection associated with this lease, if any.
        peer: Option<ZakuraConnId>,
        /// Bytes granted.
        bytes: u64,
        /// Consumer's total leased bytes after this grant.
        consumer_leased: u64,
        /// Consumer's effective cap at grant time.
        consumer_cap: u64,
        /// Consumer's protected share (`floor + share_of_S`) at grant time.
        consumer_share: u64,
        /// Pool-wide total leased bytes after this grant.
        total_leased: u64,
        /// Pool-wide lease limit (`total_floors + S`).
        pool_limit: u64,
    },
    /// An existing lease changed size (shrink or grow).
    Resized {
        /// Lease identifier.
        lease_id: u64,
        /// Consumer owning the lease.
        consumer: Consumer,
        /// Peer connection associated with this lease, if any.
        peer: Option<ZakuraConnId>,
        /// Bytes before resize.
        old_bytes: u64,
        /// Bytes after resize.
        new_bytes: u64,
        /// Consumer's total leased bytes after this resize.
        consumer_leased: u64,
        /// Consumer's effective cap at resize time.
        consumer_cap: u64,
        /// Consumer's protected share (`floor + share_of_S`) at resize time.
        consumer_share: u64,
        /// Pool-wide total leased bytes after this resize.
        total_leased: u64,
        /// Pool-wide lease limit (`total_floors + S`).
        pool_limit: u64,
    },
    /// Bytes moved atomically from one lease to a newly split child lease
    /// (zero net change to pool totals).
    Split {
        /// Source lease identifier.
        from_lease_id: u64,
        /// Newly created child lease identifier.
        to_lease_id: u64,
        /// Consumer owning both leases.
        consumer: Consumer,
        /// Peer connection associated with the child lease, if any.
        peer: Option<ZakuraConnId>,
        /// Bytes moved to `to_lease_id`.
        bytes: u64,
    },
    /// Two leases of the same consumer merged atomically (zero net change to
    /// pool totals).
    Merged {
        /// Surviving lease identifier.
        into_lease_id: u64,
        /// Consumed lease identifier (now closed).
        from_lease_id: u64,
        /// Consumer owning both leases.
        consumer: Consumer,
        /// Peer connection associated with the surviving lease, if any.
        peer: Option<ZakuraConnId>,
        /// Bytes moved into `into_lease_id`.
        bytes: u64,
    },
    /// Bytes moved atomically from one lease to a new lease of another
    /// consumer (zero net change to pool totals). The target may end above
    /// its cap: a move adds no ledger bytes.
    Moved {
        /// Source lease identifier.
        from_lease_id: u64,
        /// Newly created lease identifier.
        to_lease_id: u64,
        /// Consumer that gave the bytes.
        from: Consumer,
        /// Consumer that took the bytes.
        to: Consumer,
        /// Peer connection associated with both leases, if any.
        peer: Option<ZakuraConnId>,
        /// Bytes moved to `to_lease_id`.
        bytes: u64,
        /// `from`'s total leased bytes after the move.
        from_leased: u64,
        /// `to`'s total leased bytes after the move.
        to_leased: u64,
        /// Pool-wide total leased bytes (unchanged by the move).
        total_leased: u64,
        /// Pool-wide lease limit (`total_floors + S`).
        pool_limit: u64,
    },
    /// A lease was dropped and returned its bytes.
    Released {
        /// Lease identifier.
        lease_id: u64,
        /// Consumer owning the lease.
        consumer: Consumer,
        /// Peer connection associated with this lease, if any.
        peer: Option<ZakuraConnId>,
        /// Bytes returned.
        bytes: u64,
        /// Consumer's total leased bytes after release.
        consumer_leased: u64,
        /// Pool-wide total leased bytes after release.
        total_leased: u64,
        /// Pool-wide lease limit (`total_floors + S`).
        pool_limit: u64,
    },
    /// A block-sync lease named the block height it funds, or stopped funding
    /// one. The simulator joins leases to heights through it (`mem-15`).
    Labeled {
        /// Lease identifier.
        lease_id: u64,
        /// The height the lease funds. `None` marks a drain that a move left
        /// on its consumer because the target had no room.
        height: Option<u32>,
    },
    /// A caller started waiting on pool contention.
    Waited {
        /// Consumer that waits.
        consumer: Consumer,
        /// Peer connection associated with the attempt, if any.
        peer: Option<ZakuraConnId>,
        /// Bytes requested.
        requested_bytes: u64,
        /// Which pool rule refused the request.
        reason: LeaseDenied,
    },
}

/// Synchronous observer for pool lease transitions in test and simulator builds
/// (`03 §4`).
///
/// Called while the pool lock is held, so events arrive in the order the lock
/// serializes them. Implementations must not block, yield, take a `zakura_rt`
/// lock, or call back into [`MemoryPool`].
#[cfg(any(test, zakura_sim))]
pub trait PoolObserver: Send + Sync + fmt::Debug {
    /// Record one lease transition.
    fn observe(&self, event: PoolEvent);
}

/// One lease request: a consumer's bytes, and for serving the peer whose part
/// of serving pays for them.
#[derive(Clone, Copy, Debug)]
struct Request<'a> {
    consumer: Consumer,
    conn: Option<ZakuraConnId>,
    serving: Option<ServingTarget<'a>>,
    bytes: u64,
}

impl<'a> Request<'a> {
    /// A request of `bytes` for `consumer` outside serving.
    fn new(consumer: Consumer, conn: Option<ZakuraConnId>, bytes: u64) -> Self {
        Self {
            consumer,
            conn,
            serving: None,
            bytes,
        }
    }

    /// A request to grow `lease` by `bytes`.
    fn regrow(lease: &'a PoolLease, bytes: u64) -> Self {
        Self {
            consumer: lease.consumer,
            conn: lease.peer,
            serving: lease.serving.as_ref().map(ServingTarget::existing),
            bytes,
        }
    }
}

/// The serving peer a `Consumer::Serving` request charges.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ServingTarget<'a> {
    /// The peer the response goes to.
    pub(crate) peer: &'a ZakuraPeerId,
    /// The peer's floor: one largest response of the requesting reactor.
    pub(crate) floor: u64,
    /// The cap a newly seen peer starts with.
    pub(crate) cap: u64,
}

/// One async waiter's registration in [`PoolState`].
#[derive(Clone, Debug, Eq, PartialEq)]
struct WaitSlot {
    consumer: Consumer,
    class: WakeClass,
    /// Waiting on node-level contention: rule 3 counts it.
    node_contended: bool,
    /// The serving peer waiting on contention at either level: while it
    /// waits below its part, peers at or above theirs get nothing new.
    waiting_peer: Option<PeerKey>,
}

impl WaitSlot {
    fn contended(&self) -> bool {
        self.node_contended || self.waiting_peer.is_some()
    }
}

/// Everything the pool lock guards: the node machine and serving's per-peer
/// sub-pool, so serving's two levels decide as one.
#[derive(Debug)]
struct PoolState {
    machine: MemoryPoolMachine,
    serving: PeerSubPool,
    /// Lease identifiers follow lock order, so the simulator replays them.
    #[cfg(any(test, zakura_sim))]
    next_lease_id: u64,
}

impl PoolState {
    /// Decide `request`, taking it on both levels when granted.
    fn lease(&mut self, request: &Request<'_>) -> Result<Option<PeerKey>, (Refusal, Level)> {
        match request.serving {
            None => self
                .machine
                .try_lease(request.consumer, request.bytes)
                .map(|()| None)
                .map_err(|refusal| (refusal, Level::Node)),
            Some(target) => {
                debug_assert_eq!(request.consumer, Consumer::Serving);
                self.serving.ensure(target.peer, target.floor, target.cap);
                try_serving_lease(
                    &mut self.machine,
                    &mut self.serving,
                    target.peer,
                    request.bytes,
                )
                .map(Some)
            }
        }
    }

    /// Return `bytes` of `consumer`, and of `serving`'s part when set.
    fn release(&mut self, consumer: Consumer, serving: Option<&PeerKey>, bytes: u64) -> Released {
        let released = self.machine.release(consumer, bytes);
        if let Some(key) = serving {
            self.serving.release(key.peer(), released.bytes);
        }
        released
    }

    /// The registration for a waiter refused with `refusal` at `level`.
    fn wait_slot(&self, request: &Request<'_>, refusal: Refusal, level: Level) -> WaitSlot {
        let contended = matches!(refusal, Refusal::Contended(_));
        match request.serving {
            Some(target) => WaitSlot {
                consumer: request.consumer,
                class: WakeClass::of(
                    self.serving
                        .is_below_share(target.peer, serving_ceiling(&self.machine)),
                ),
                node_contended: contended && level == Level::Node,
                waiting_peer: contended.then(|| self.serving.key(target.peer)).flatten(),
            },
            None => WaitSlot {
                consumer: request.consumer,
                class: WakeClass::of(self.machine.is_below_share(request.consumer)),
                node_contended: contended,
                waiting_peer: None,
            },
        }
    }

    fn add_slot(&mut self, slot: &WaitSlot) {
        self.machine
            .add_waiter(slot.consumer, slot.class, slot.node_contended);
        if let Some(peer) = &slot.waiting_peer {
            self.serving.add_waiter(peer.peer());
        }
    }

    fn remove_slot(&mut self, slot: &WaitSlot) {
        self.machine
            .remove_waiter(slot.consumer, slot.class, slot.node_contended);
        if let Some(peer) = &slot.waiting_peer {
            self.serving.remove_waiter(peer.peer());
        }
    }

    /// The tag of a newly granted lease.
    fn grant_tag(&mut self) -> LeaseTag {
        LeaseTag {
            #[cfg(any(test, zakura_sim))]
            id: self.alloc_lease_id(),
            #[cfg(any(test, zakura_sim))]
            height: None,
        }
    }

    #[cfg(any(test, zakura_sim))]
    fn alloc_lease_id(&mut self) -> u64 {
        let id = self.next_lease_id;
        self.next_lease_id += 1;
        id
    }
}

/// Metric handles, resolved once so lease operations only store values.
struct PoolMetrics {
    leased: [metrics::Gauge; Consumer::COUNT],
    cap: [metrics::Gauge; Consumer::COUNT],
    waits: [metrics::Counter; Consumer::COUNT],
}

impl PoolMetrics {
    fn new() -> Self {
        Self {
            leased: Consumer::ALL.map(|consumer| {
                metrics::gauge!("zakura.memory.leased_bytes", "consumer" => consumer.label())
            }),
            cap: Consumer::ALL.map(|consumer| {
                metrics::gauge!("zakura.memory.cap_bytes", "consumer" => consumer.label())
            }),
            waits: Consumer::ALL.map(|consumer| {
                metrics::counter!("zakura.memory.waits_total", "consumer" => consumer.label())
            }),
        }
    }
}

/// What one critical section changed, and its side effects, which apply
/// after unlocking.
#[derive(Debug, Default)]
struct Effects {
    /// A consumer that returned bytes or gained room (see
    /// [`MemoryPoolMachine::wake`]).
    released_by: Option<Consumer>,
    /// Whether bytes returned to `S`.
    freed_shared: bool,
    wake: Wake,
    /// A consumer's leased bytes to publish.
    leased: Option<(Consumer, u64)>,
    /// A consumer's cap to publish.
    cap: Option<(Consumer, u64)>,
    /// The leased bytes of the consumer a move filled, to publish.
    moved_into: Option<(Consumer, u64)>,
    /// A consumer that started waiting on contention.
    waited: Option<Consumer>,
    overrun: Option<OverrunReport>,
}

struct MemoryPoolInner {
    state: Mutex<PoolState>,
    capacity_version: AtomicU64,
    /// Async waiters, per [`WakeClass`] and consumer, woken one at a time.
    waiters: [[Notify; Consumer::COUNT]; 2],
    /// Pollers per consumer ([`MemoryPool::notifier`]), woken together.
    pollers: [Notify; Consumer::COUNT],
    metrics: PoolMetrics,
    #[cfg(any(test, zakura_sim))]
    draining_block_receive_bytes: Arc<CounterU64>,
    #[cfg(any(test, zakura_sim))]
    observer: Option<Arc<dyn PoolObserver>>,
}

impl fmt::Debug for MemoryPoolInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryPoolInner")
            .field(
                "capacity_version",
                &self.capacity_version.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

/// Outcome of one attempt in [`MemoryPool::wait_for_lease`].
enum Attempt {
    Granted(PoolLease),
    Wait(WakeClass),
    Final(LeaseDenied),
}

/// Shared handle to the node-wide memory pool.
///
/// Cloning is a single `Arc` clone. Hot-path lease operations take one brief
/// uncontended lock, touch only an inline 7-element array, allocate nothing,
/// and wake nobody unless a waiter or poller can use the change.
#[derive(Clone, Debug)]
pub struct MemoryPool {
    inner: Arc<MemoryPoolInner>,
}

impl MemoryPool {
    /// Create a new memory pool from `config` and publish initial cap gauges.
    pub fn new(config: PoolConfig) -> Self {
        let machine = MemoryPoolMachine::new(config);
        let metrics = PoolMetrics::new();
        for consumer in Consumer::ALL {
            metrics.cap[consumer.index()].set(gauge_value(machine.cap(consumer)));
            metrics.leased[consumer.index()].set(0.0);
        }
        Self {
            inner: Arc::new(MemoryPoolInner {
                state: Mutex::new(PoolState {
                    machine,
                    serving: PeerSubPool::new(),
                    #[cfg(any(test, zakura_sim))]
                    next_lease_id: 1,
                }),
                capacity_version: AtomicU64::new(0),
                waiters: std::array::from_fn(|_| std::array::from_fn(|_| Notify::new())),
                pollers: std::array::from_fn(|_| Notify::new()),
                metrics,
                #[cfg(any(test, zakura_sim))]
                draining_block_receive_bytes: Arc::new(CounterU64::new(0)),
                #[cfg(any(test, zakura_sim))]
                observer: None,
            }),
        }
    }

    /// Attach a [`PoolObserver`] before cloning this pool.
    #[cfg(any(test, zakura_sim))]
    pub fn with_observer(mut self, observer: Option<Arc<dyn PoolObserver>>) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("a memory pool gets its observer before anything clones it")
            .observer = observer;
        self
    }

    /// Preemption-free atomic counter of `BlockSync` + `Consensus` receive-credit
    /// bytes currently held in `ConnectionCredit`'s QUIC drain queues (`02 §6`).
    #[cfg(any(test, zakura_sim))]
    pub fn draining_block_receive_bytes_atomic(&self) -> Arc<CounterU64> {
        self.inner.draining_block_receive_bytes.clone()
    }

    #[cfg(any(test, zakura_sim))]
    pub(crate) fn add_draining_receive_bytes(&self, consumer: Consumer, bytes: u64) {
        if bytes > 0 && matches!(consumer, Consumer::BlockSync | Consumer::Consensus) {
            self.inner
                .draining_block_receive_bytes
                .fetch_add(bytes, Ordering::AcqRel);
        }
    }

    #[cfg(any(test, zakura_sim))]
    pub(crate) fn sub_draining_receive_bytes(&self, consumer: Consumer, bytes: u64) {
        if bytes > 0 && matches!(consumer, Consumer::BlockSync | Consumer::Consensus) {
            self.inner
                .draining_block_receive_bytes
                .fetch_sub(bytes, Ordering::AcqRel);
        }
    }

    /// Monotonic version bumped whenever the pool wakes anyone. A poller reads
    /// it before trying, then again after enabling [`Self::notifier`], and
    /// retries at once if it moved.
    #[inline]
    pub fn capacity_version(&self) -> u64 {
        self.inner.capacity_version.load(Ordering::Acquire)
    }

    /// Notification for callers that poll `consumer`. It fires once after a
    /// refused [`Self::try_acquire`] when a later change can help, and on
    /// every such change while a [`PoolWaitToken`] is held.
    #[inline]
    pub fn notifier(&self, consumer: Consumer) -> &Notify {
        &self.inner.pollers[consumer.index()]
    }

    /// Create a zero-byte lease bound to this pool without locking or emitting
    /// an observer event.
    pub fn empty_lease(&self, consumer: Consumer, peer: Option<ZakuraConnId>) -> PoolLease {
        PoolLease::new(self, consumer, peer, None, 0, LeaseTag::default())
    }

    /// Try to acquire a lease of `bytes` for `consumer` without waiting.
    ///
    /// A refusal doesn't mark `consumer` waiting. A caller that will retry
    /// holds a [`PoolWaitToken`] while it does.
    pub fn try_acquire(
        &self,
        consumer: Consumer,
        peer: Option<ZakuraConnId>,
        bytes: u64,
    ) -> Result<PoolLease, LeaseDenied> {
        self.try_request(&Request::new(consumer, peer, bytes))
    }

    /// Wait until `bytes` can be leased for `consumer`, without holding any
    /// lock across the await (`02 §5`).
    ///
    /// Waits through pool contention, marking `consumer` waiting, and through
    /// `consumer`'s own cap, marking nothing. Returns the refusal at once when
    /// waiting can never help ([`Refusal::Final`]).
    pub async fn acquire(
        &self,
        consumer: Consumer,
        peer: Option<ZakuraConnId>,
        bytes: u64,
    ) -> Result<PoolLease, LeaseDenied> {
        let request = Request::new(consumer, peer, bytes);
        self.wait_for_lease(&request, std::future::pending::<LeaseDenied>())
            .await
    }

    /// Acquire a connection's transport base `t` and negotiated reactor session
    /// bases `b_r`, `bytes` in all, as one `Consumer::Sessions` lease
    /// (`02 §8` rules 1–3, `mem-13`).
    ///
    /// - Refuses at once when `Sessions` is at its share or cap: it never waits
    ///   for healthy connections to disconnect.
    /// - When `Sessions` is below its share but short-lived bulk consumers
    ///   borrowed `S`, waits on contention until `S` returns, `wait_timeout`
    ///   expires, or `shutdown` cancels; the last two refuse with `PoolFull`.
    ///
    /// The connection holds the lease until it closes. A session that a
    /// service admits later grows it ([`PoolLease::try_grow_to`]).
    pub async fn acquire_session_lease(
        &self,
        conn_id: ZakuraConnId,
        bytes: u64,
        shutdown: &zakura_rt::sync::CancellationToken,
        wait_timeout: std::time::Duration,
    ) -> Result<PoolLease, LeaseDenied> {
        let request = Request::new(Consumer::Sessions, Some(conn_id), bytes);
        let stop = async {
            zakura_rt::select! {
                () = shutdown.cancelled() => {}
                () = zakura_rt::time::sleep(wait_timeout) => {}
            }
            LeaseDenied::PoolFull
        };
        self.wait_for_lease(&request, stop).await
    }

    /// Mark `consumer` waiting on pool contention until the token drops.
    ///
    /// For callers that retry by polling [`Self::notifier`] instead of awaiting
    /// [`Self::acquire`]: while the token lives, rule 3 holds other consumers
    /// at or above their shares back for `consumer`, and every change that can
    /// help wakes the notifier.
    pub fn wait_token(&self, consumer: Consumer) -> PoolWaitToken {
        self.lock().machine.add_token(consumer);
        PoolWaitToken {
            pool: self.clone(),
            consumer,
        }
    }

    /// Set the dynamic benefit cap of a bulk consumer, clamped between its
    /// floor and the plan's cap.
    ///
    /// Raising a cap wakes `consumer`; lowering one revokes no live lease.
    pub fn set_cap(&self, consumer: Consumer, new_cap: u64) {
        self.with_state(|state, effects| {
            let old_cap = state.machine.cap(consumer);
            state.machine.set_cap(consumer, new_cap);
            let cap = state.machine.cap(consumer);
            effects.cap = Some((consumer, cap));
            effects.released_by = (cap > old_cap).then_some(consumer);
        });
    }

    /// Current live ceiling available to `consumer`.
    pub fn available_ceiling(&self, consumer: Consumer) -> u64 {
        self.lock().machine.available_ceiling(consumer)
    }

    /// Protected total (`floor + share_of_S`) for `consumer`.
    pub fn protected_share(&self, consumer: Consumer) -> u64 {
        self.lock().machine.protected_share(consumer)
    }

    /// Bytes currently leased by `consumer`.
    pub fn consumer_leased(&self, consumer: Consumer) -> u64 {
        self.lock().machine.leased(consumer)
    }

    /// Additional bytes `consumer` can currently acquire under its live ceiling.
    pub fn available_for(&self, consumer: Consumer) -> u64 {
        let state = self.lock();
        state
            .machine
            .available_ceiling(consumer)
            .saturating_sub(state.machine.leased(consumer))
    }

    /// Take an immutable snapshot of the pool state.
    pub fn snapshot(&self) -> PoolSnapshot {
        self.lock().machine.snapshot()
    }

    /// Check and log any live budget overrun using the current clock.
    pub fn check_overrun(&self) -> Option<OverrunReport> {
        let report = self.lock().machine.check_overrun(Instant::now());
        if let Some(report) = report {
            log_overrun(report);
        }
        report
    }

    /// Register one serving connection of `peer` (see [`super::ServingPool`]).
    pub(crate) fn register_serving_peer(&self, peer: &ZakuraPeerId, floor: u64, cap: u64) {
        self.lock().serving.register(peer, floor, cap);
    }

    /// Unregister one serving connection of `peer`. The other peers' parts
    /// grow, so serving's waiters wake.
    pub(crate) fn unregister_serving_peer(&self, peer: &ZakuraPeerId) {
        self.with_state(|state, effects| {
            state.serving.unregister(peer);
            effects.released_by = Some(Consumer::Serving);
        });
    }

    /// Set `peer`'s serving cap. Raising it wakes serving's waiters.
    pub(crate) fn set_serving_peer_cap(&self, peer: &ZakuraPeerId, cap: u64) {
        self.with_state(|state, effects| {
            effects.released_by = state
                .serving
                .set_cap(peer, cap)
                .then_some(Consumer::Serving);
        });
    }

    /// Bytes `peer` holds in serving's sub-pool.
    #[cfg(test)]
    pub(crate) fn serving_peer_leased(&self, peer: &ZakuraPeerId) -> u64 {
        self.lock().serving.peer_leased(peer)
    }

    /// Bytes every serving peer holds in serving's sub-pool.
    #[cfg(test)]
    pub(crate) fn serving_total_leased(&self) -> u64 {
        self.lock().serving.total_leased()
    }

    /// Try a serving lease for `target` without waiting.
    pub(crate) fn try_acquire_serving(
        &self,
        target: ServingTarget<'_>,
        conn: Option<ZakuraConnId>,
        bytes: u64,
    ) -> Result<PoolLease, LeaseDenied> {
        self.try_request(&Request {
            consumer: Consumer::Serving,
            conn,
            serving: Some(target),
            bytes,
        })
    }

    /// Wait for a serving lease for `target` (see [`Self::acquire`]).
    pub(crate) async fn acquire_serving(
        &self,
        target: ServingTarget<'_>,
        conn: Option<ZakuraConnId>,
        bytes: u64,
    ) -> Result<PoolLease, LeaseDenied> {
        let request = Request {
            consumer: Consumer::Serving,
            conn,
            serving: Some(target),
            bytes,
        };
        self.wait_for_lease(&request, std::future::pending::<LeaseDenied>())
            .await
    }

    fn try_request(&self, request: &Request<'_>) -> Result<PoolLease, LeaseDenied> {
        if request.bytes == 0 {
            return Ok(self.empty_lease(request.consumer, request.conn));
        }
        self.with_state(|state, effects| {
            let result = match state.lease(request) {
                Ok(serving) => Ok(self.grant(state, request, serving, effects)),
                Err((refusal, _)) => Err(refusal.reason()),
            };
            if let Some(target) = request.serving {
                state.serving.remove_if_idle(target.peer);
            }
            result
        })
    }

    /// The one wait loop: try, register as a waiter on refusal, and sleep
    /// until a change wakes this waiter or `stop` resolves to the refusal to
    /// return. No lock is held across an await.
    async fn wait_for_lease(
        &self,
        request: &Request<'_>,
        stop: impl Future<Output = LeaseDenied>,
    ) -> Result<PoolLease, LeaseDenied> {
        if request.bytes == 0 {
            return Ok(self.empty_lease(request.consumer, request.conn));
        }
        zakura_rt::pin!(stop);
        let mut registration = WaitRegistration {
            pool: self,
            slot: None,
        };
        let mut woken = false;
        loop {
            let (attempt, version) = self.attempt(request, &mut registration.slot, woken);
            let class = match attempt {
                Attempt::Granted(lease) => return Ok(lease),
                Attempt::Final(reason) => return Err(reason),
                Attempt::Wait(class) => class,
            };
            let notified = self.inner.waiters[class.index()][request.consumer.index()].notified();
            zakura_rt::pin!(notified);
            notified.as_mut().enable();
            // A wakeup between the attempt and `enable` bumped the version.
            if self.capacity_version() != version {
                woken = false;
                continue;
            }
            zakura_rt::select! {
                biased;
                reason = &mut stop => return Err(reason),
                () = notified => woken = true,
            }
        }
    }

    /// One attempt of [`Self::wait_for_lease`]. Updates `slot` under the same
    /// lock as the decision, passes the turn on when a wakeup caused this
    /// attempt, and returns the capacity version that lock saw.
    fn attempt(
        &self,
        request: &Request<'_>,
        slot: &mut Option<WaitSlot>,
        woken: bool,
    ) -> (Attempt, u64) {
        self.with_state(|state, effects| {
            let version = self.inner.capacity_version.load(Ordering::Acquire);
            let attempt = match state.lease(request) {
                Ok(serving) => {
                    let lease = self.grant(state, request, serving, effects);
                    if let Some(old) = slot.take() {
                        state.remove_slot(&old);
                    }
                    Attempt::Granted(lease)
                }
                Err((Refusal::Final(reason), _)) => {
                    if let Some(old) = slot.take() {
                        state.remove_slot(&old);
                    }
                    Attempt::Final(reason)
                }
                Err((refusal, level)) => {
                    let next = state.wait_slot(request, refusal, level);
                    let class = next.class;
                    if slot.as_ref() != Some(&next) {
                        let was_contended = slot.as_ref().is_some_and(WaitSlot::contended);
                        if let Some(old) = slot.take() {
                            state.remove_slot(&old);
                        }
                        state.add_slot(&next);
                        if next.contended() && !was_contended {
                            effects.waited = Some(request.consumer);
                            #[cfg(any(test, zakura_sim))]
                            self.emit(PoolEvent::Waited {
                                consumer: request.consumer,
                                peer: request.conn,
                                requested_bytes: request.bytes,
                                reason: refusal.reason(),
                            });
                        }
                        *slot = Some(next);
                    }
                    Attempt::Wait(class)
                }
            };
            if let Some(target) = request.serving {
                state.serving.remove_if_idle(target.peer);
            }
            if woken {
                effects
                    .wake
                    .merge(state.machine.take_turn(request.consumer));
            }
            (attempt, version)
        })
    }

    /// Build the lease for a granted `request` and record the grant.
    fn grant(
        &self,
        state: &mut PoolState,
        request: &Request<'_>,
        serving: Option<PeerKey>,
        effects: &mut Effects,
    ) -> PoolLease {
        let consumer = request.consumer;
        let _consumer_leased = record_grant(state, consumer, effects);
        let tag = state.grant_tag();
        #[cfg(any(test, zakura_sim))]
        self.emit(PoolEvent::Granted {
            lease_id: tag.id,
            consumer,
            peer: request.conn,
            bytes: request.bytes,
            consumer_leased: _consumer_leased,
            consumer_cap: state.machine.cap(consumer),
            consumer_share: state.machine.protected_share(consumer),
            total_leased: state.machine.total_leased(),
            pool_limit: state.machine.lease_limit(),
        });
        PoolLease::new(self, consumer, request.conn, serving, request.bytes, tag)
    }

    /// Return `lease`'s bytes above `new_bytes` to the pool.
    fn shrink_lease(&self, lease: &PoolLease, new_bytes: u64) {
        self.with_state(|state, effects| {
            let released = state.release(
                lease.consumer,
                lease.serving.as_ref(),
                lease.bytes - new_bytes,
            );
            let consumer_leased = state.machine.leased(lease.consumer);
            effects.leased = Some((lease.consumer, consumer_leased));
            effects.released_by = Some(lease.consumer);
            effects.freed_shared = released.freed_shared > 0;
            #[cfg(any(test, zakura_sim))]
            self.emit(if new_bytes == 0 {
                PoolEvent::Released {
                    lease_id: lease.tag.id,
                    consumer: lease.consumer,
                    peer: lease.peer,
                    bytes: released.bytes,
                    consumer_leased,
                    total_leased: state.machine.total_leased(),
                    pool_limit: state.machine.lease_limit(),
                }
            } else {
                PoolEvent::Resized {
                    lease_id: lease.tag.id,
                    consumer: lease.consumer,
                    peer: lease.peer,
                    old_bytes: lease.bytes,
                    new_bytes,
                    consumer_leased,
                    consumer_cap: state.machine.cap(lease.consumer),
                    consumer_share: state.machine.protected_share(lease.consumer),
                    total_leased: state.machine.total_leased(),
                    pool_limit: state.machine.lease_limit(),
                }
            });
        });
    }

    /// Grow `lease` to `new_bytes` without waiting.
    fn grow_lease(&self, lease: &PoolLease, new_bytes: u64) -> Result<(), LeaseDenied> {
        let request = Request::regrow(lease, new_bytes - lease.bytes);
        self.with_state(|state, effects| {
            state
                .lease(&request)
                .map_err(|(refusal, _)| refusal.reason())?;
            let _consumer_leased = record_grant(state, lease.consumer, effects);
            #[cfg(any(test, zakura_sim))]
            self.emit(PoolEvent::Resized {
                lease_id: lease.tag.id,
                consumer: lease.consumer,
                peer: lease.peer,
                old_bytes: lease.bytes,
                new_bytes,
                consumer_leased: _consumer_leased,
                consumer_cap: state.machine.cap(lease.consumer),
                consumer_share: state.machine.protected_share(lease.consumer),
                total_leased: state.machine.total_leased(),
                pool_limit: state.machine.lease_limit(),
            });
            Ok(())
        })
    }

    /// Move up to `lease`'s bytes to `to` (see [`MemoryPoolMachine::transfer`]),
    /// and return the bytes moved and the new lease's identifier.
    fn move_lease(&self, lease: &PoolLease, to: Consumer) -> (u64, u64) {
        debug_assert!(lease.serving.is_none(), "serving leases never move");
        self.with_state(|state, effects| {
            let shared_before = state.machine.shared_used();
            let moved = state.machine.transfer(lease.consumer, to, lease.bytes);
            if moved == 0 {
                return (0, 0);
            }
            effects.leased = Some((lease.consumer, state.machine.leased(lease.consumer)));
            effects.moved_into = Some((to, state.machine.leased(to)));
            effects.released_by = Some(lease.consumer);
            effects.freed_shared = state.machine.shared_used() < shared_before;
            #[cfg(any(test, zakura_sim))]
            let to_lease_id = {
                let to_lease_id = state.alloc_lease_id();
                self.emit(PoolEvent::Moved {
                    from_lease_id: lease.tag.id,
                    to_lease_id,
                    from: lease.consumer,
                    to,
                    peer: lease.peer,
                    bytes: moved,
                    from_leased: state.machine.leased(lease.consumer),
                    to_leased: state.machine.leased(to),
                    total_leased: state.machine.total_leased(),
                    pool_limit: state.machine.lease_limit(),
                });
                to_lease_id
            };
            #[cfg(not(any(test, zakura_sim)))]
            let to_lease_id = 0;
            (moved, to_lease_id)
        })
    }

    /// Run one critical section. `change` updates the pool state and records
    /// what it changed in `effects`. Then, under the same lock, the machine
    /// decides whose waiters the change wakes: rule 3's state before the
    /// change is read first, so a change that stops rule 3 holding consumers
    /// back wakes them. Metrics, the overrun warning and wakeups apply after
    /// unlocking.
    fn with_state<R>(&self, change: impl FnOnce(&mut PoolState, &mut Effects) -> R) -> R {
        let mut effects = Effects::default();
        let out = {
            let mut state = self.lock();
            let rule3_before = state.machine.any_below_share_waiting_except(None);
            let out = change(&mut state, &mut effects);
            let wake = state
                .machine
                .wake(effects.released_by, effects.freed_shared, rule3_before);
            effects.wake.merge(wake);
            out
        };
        self.apply(effects);
        out
    }

    #[inline]
    fn lock(&self) -> MutexGuard<'_, PoolState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn apply(&self, effects: Effects) {
        let metrics = &self.inner.metrics;
        for (consumer, bytes) in effects.leased.into_iter().chain(effects.moved_into) {
            metrics.leased[consumer.index()].set(gauge_value(bytes));
        }
        if let Some((consumer, cap)) = effects.cap {
            metrics.cap[consumer.index()].set(gauge_value(cap));
        }
        if let Some(consumer) = effects.waited {
            metrics.waits[consumer.index()].increment(1);
        }
        if let Some(report) = effects.overrun {
            log_overrun(report);
        }
        self.wake(effects.wake);
    }

    fn wake(&self, wake: Wake) {
        if wake.is_empty() {
            return;
        }
        self.inner.capacity_version.fetch_add(1, Ordering::AcqRel);
        for consumer in Consumer::ALL {
            for class in WakeClass::ALL {
                if wake.wakes_waiter(consumer, class) {
                    self.inner.waiters[class.index()][consumer.index()].notify_one();
                }
            }
            if wake.wakes_pollers(consumer) {
                self.inner.pollers[consumer.index()].notify_waiters();
            }
        }
    }

    #[cfg(any(test, zakura_sim))]
    #[inline]
    fn emit(&self, event: PoolEvent) {
        if let Some(observer) = &self.inner.observer {
            observer.observe(event);
        }
    }
}

impl<'a> ServingTarget<'a> {
    /// The target of growing a lease that `key`'s peer already holds.
    fn existing(key: &'a PeerKey) -> Self {
        Self {
            peer: key.peer(),
            floor: 0,
            cap: 0,
        }
    }
}

/// Unregisters a [`MemoryPool::wait_for_lease`] waiter when the wait ends or
/// is cancelled.
struct WaitRegistration<'a> {
    pool: &'a MemoryPool,
    slot: Option<WaitSlot>,
}

impl Drop for WaitRegistration<'_> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            // Rule 3 may have held consumers back for this waiter.
            self.pool.with_state(|state, _| state.remove_slot(&slot));
        }
    }
}

/// Marks a consumer waiting on pool contention until dropped.
///
/// See [`MemoryPool::wait_token`].
#[derive(Debug)]
#[must_use = "the consumer stops waiting when the token drops"]
pub struct PoolWaitToken {
    pool: MemoryPool,
    consumer: Consumer,
}

impl Drop for PoolWaitToken {
    fn drop(&mut self) {
        let consumer = self.consumer;
        // Rule 3 may have held consumers back for this token.
        self.pool
            .with_state(|state, _| state.machine.remove_token(consumer));
    }
}

/// A lease's identity for test and simulator observers. Production builds
/// carry nothing.
#[derive(Clone, Copy, Debug, Default)]
struct LeaseTag {
    /// Unique lease identifier within the pool, 0 while the lease is empty.
    #[cfg(any(test, zakura_sim))]
    id: u64,
    /// The block height this lease funds, for the simulator's `mem-15` check.
    #[cfg(any(test, zakura_sim))]
    height: Option<u32>,
}

/// RAII handle owning a logical-byte lease in a [`MemoryPool`].
///
/// Dropping a `PoolLease` returns its bytes to the pool and wakes waiters
/// that can use them. Zero-byte leases do not lock on drop.
#[must_use = "retain the lease while its bytes are held"]
pub struct PoolLease {
    pool: MemoryPool,
    consumer: Consumer,
    peer: Option<ZakuraConnId>,
    /// The serving peer whose part of serving also holds these bytes.
    serving: Option<PeerKey>,
    bytes: u64,
    tag: LeaseTag,
}

impl fmt::Debug for PoolLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolLease")
            .field("consumer", &self.consumer)
            .field("peer", &self.peer)
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl PoolLease {
    /// A handle for `bytes` that `pool` already charged to `consumer`.
    fn new(
        pool: &MemoryPool,
        consumer: Consumer,
        peer: Option<ZakuraConnId>,
        serving: Option<PeerKey>,
        bytes: u64,
        tag: LeaseTag,
    ) -> Self {
        Self {
            pool: pool.clone(),
            consumer,
            peer,
            serving,
            bytes,
            tag,
        }
    }

    /// Consumer charged for this lease.
    #[inline]
    pub fn consumer(&self) -> Consumer {
        self.consumer
    }

    /// Connection associated with this lease, if any.
    #[inline]
    pub fn peer(&self) -> Option<ZakuraConnId> {
        self.peer
    }

    /// Bytes currently held by this lease.
    #[inline]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Pool that owns this lease.
    #[inline]
    pub fn pool(&self) -> &MemoryPool {
        &self.pool
    }

    /// Unique lease identifier for test/simulator observers.
    #[cfg(any(test, zakura_sim))]
    #[inline]
    pub fn lease_id(&self) -> u64 {
        self.tag.id
    }

    /// Name `height` as the block height this lease funds, and report it to
    /// the observer. Splits, moves and regrants keep the name. Only test and
    /// simulator builds keep it, for the simulator's `mem-15` check.
    #[inline]
    pub fn label_height(&mut self, height: u32) {
        #[cfg(any(test, zakura_sim))]
        {
            self.tag.height = Some(height);
            self.report_height();
        }
        #[cfg(not(any(test, zakura_sim)))]
        let _ = height;
    }

    /// The block height this lease funds, if a caller named one. Production
    /// builds keep no label, so they always return `None`.
    #[inline]
    pub fn height(&self) -> Option<u32> {
        #[cfg(any(test, zakura_sim))]
        return self.tag.height;
        #[cfg(not(any(test, zakura_sim)))]
        None
    }

    /// Report this lease's height to the observer, if the lease is live.
    ///
    /// Only the lease's owner reports its label, so the report needs no
    /// lock to stay in order with the lease's other events. Taking none adds
    /// no scheduling point to a simulated run.
    #[cfg(any(test, zakura_sim))]
    fn report_height(&self) {
        if self.tag.id == 0 {
            return;
        }
        self.pool.emit(PoolEvent::Labeled {
            lease_id: self.tag.id,
            height: self.tag.height,
        });
    }

    /// Shrink this lease down to `new_bytes`, returning the excess to the pool.
    ///
    /// A `new_bytes >= self.bytes` is a no-op (zero locks).
    pub fn shrink_to(&mut self, new_bytes: u64) {
        if new_bytes >= self.bytes {
            return;
        }
        self.pool.shrink_lease(self, new_bytes);
        self.bytes = new_bytes;
        #[cfg(any(test, zakura_sim))]
        if new_bytes == 0 {
            self.tag.id = 0;
        }
    }

    /// Shrink this lease by `delta` bytes (clamped to `self.bytes`).
    pub fn shrink_by(&mut self, delta: u64) {
        self.shrink_to(self.bytes.saturating_sub(delta));
    }

    /// Try to grow this lease to `target_bytes` without waiting. A refusal
    /// marks nothing waiting.
    ///
    /// If `target_bytes <= self.bytes`, this shrinks (or does nothing) and
    /// returns `Ok(())`.
    pub fn try_grow_to(&mut self, target_bytes: u64) -> Result<(), LeaseDenied> {
        if target_bytes <= self.bytes {
            self.shrink_to(target_bytes);
            return Ok(());
        }
        if self.bytes == 0 {
            // A regrant from zero is a new lease with a fresh identifier,
            // which keeps the height label.
            let height = self.height();
            *self = self
                .pool
                .try_request(&Request::regrow(self, target_bytes))?;
            if let Some(height) = height {
                self.label_height(height);
            }
            return Ok(());
        }
        self.pool.grow_lease(self, target_bytes)?;
        self.bytes = target_bytes;
        Ok(())
    }

    /// Split `bytes` off this lease into a new child lease of the same consumer
    /// without releasing and reacquiring capacity.
    ///
    /// Returns `None` if `bytes > self.bytes`. Splitting 0 bytes returns an
    /// empty lease without locking.
    pub fn split(&mut self, bytes: u64) -> Option<Self> {
        if bytes == 0 {
            return Some(self.pool.empty_lease(self.consumer, self.peer));
        }
        let remaining = self.bytes.checked_sub(bytes)?;
        self.bytes = remaining;

        #[cfg(any(test, zakura_sim))]
        let tag = {
            let mut state = self.pool.lock();
            let to_lease_id = state.alloc_lease_id();
            self.pool.emit(PoolEvent::Split {
                from_lease_id: self.tag.id,
                to_lease_id,
                consumer: self.consumer,
                peer: self.peer,
                bytes,
            });
            if remaining == 0 {
                // The split moved every byte, which closes the source lease.
                self.tag.id = 0;
            }
            LeaseTag {
                id: to_lease_id,
                height: self.tag.height,
            }
        };
        #[cfg(not(any(test, zakura_sim)))]
        let tag = LeaseTag::default();

        Some(Self::new(
            &self.pool,
            self.consumer,
            self.peer,
            self.serving.clone(),
            bytes,
            tag,
        ))
    }

    /// Merge `other` into `self` without releasing and reacquiring capacity.
    ///
    /// # Panics
    ///
    /// Debug-asserts that both leases belong to the same pool, consumer and
    /// serving peer.
    pub fn merge(&mut self, mut other: Self) {
        debug_assert!(Arc::ptr_eq(&self.pool.inner, &other.pool.inner));
        debug_assert_eq!(self.consumer, other.consumer);
        let moved = std::mem::take(&mut other.bytes);
        if moved == 0 {
            return;
        }

        if self.bytes == 0 {
            self.bytes = moved;
            self.serving = other.serving.take();
            self.tag = other.tag;
            return;
        }

        debug_assert_eq!(self.serving, other.serving);
        self.bytes += moved;
        #[cfg(any(test, zakura_sim))]
        {
            let _state = self.pool.lock();
            self.pool.emit(PoolEvent::Merged {
                into_lease_id: self.tag.id,
                from_lease_id: other.tag.id,
                consumer: self.consumer,
                peer: self.peer,
                bytes: moved,
            });
        }
    }

    /// Move this lease's bytes to `to`, without releasing and reacquiring
    /// capacity, and return the moved part as a new lease of `to`.
    ///
    /// The move ignores `to`'s cap and share, but never takes more than the
    /// pool can hold (see [`MemoryPoolMachine::transfer`]). The bytes that
    /// don't fit stay on `self`. Serving leases never move.
    pub fn move_to(&mut self, to: Consumer) -> PoolLease {
        if self.bytes == 0 || to == self.consumer || self.serving.is_some() {
            return self.pool.empty_lease(to, self.peer);
        }
        let (moved, _to_lease_id) = self.pool.move_lease(self, to);
        self.bytes -= moved;
        let tag = self.tag;
        #[cfg(any(test, zakura_sim))]
        if self.bytes == 0 {
            // The move took every byte, which closes the source lease.
            self.tag.id = 0;
        } else if tag.height.is_some() {
            // The bytes left here are a drain the target had no room for.
            self.tag.height = None;
            self.report_height();
        }
        if moved == 0 {
            return self.pool.empty_lease(to, self.peer);
        }
        #[cfg(any(test, zakura_sim))]
        let tag = LeaseTag {
            id: _to_lease_id,
            ..tag
        };
        Self::new(&self.pool, to, self.peer, None, moved, tag)
    }
}

impl Drop for PoolLease {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.pool.shrink_lease(self, 0);
            self.bytes = 0;
        }
    }
}

/// Publish `consumer`'s leased bytes after a grant and check for an overrun.
/// Returns the consumer's leased bytes.
fn record_grant(state: &mut PoolState, consumer: Consumer, effects: &mut Effects) -> u64 {
    let consumer_leased = state.machine.leased(consumer);
    effects.leased = Some((consumer, consumer_leased));
    if state.machine.is_over_budget() {
        effects.overrun = state.machine.check_overrun(Instant::now());
    }
    consumer_leased
}

/// A byte count as a gauge value.
#[inline]
fn gauge_value(bytes: u64) -> f64 {
    // Metrics only: the cast rounds above 2^53 bytes, which only blurs a gauge.
    bytes as f64
}

fn log_overrun(report: OverrunReport) {
    tracing::warn!(
        live_bytes = report.live_bytes,
        budget_bytes = report.budget_bytes,
        excess_bytes = report.excess_bytes,
        "live P2P memory exceeds [memory] budget due to operator overrides"
    );
}
