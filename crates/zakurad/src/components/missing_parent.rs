//! Reserve active download capacity for blocks with a usable committed parent.
//!
//! This is download admission policy, not a consensus rule. Each downloader owns
//! independent accounting. Deferred work retains hashes, never block bodies.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use futures::future::join_all;
use tokio::{
    sync::Notify,
    time::{timeout, Instant},
};
use tower::{Service, ServiceExt};
use zakura_chain::{
    block::{self, Height},
    chain_tip::ChainTip,
};
use zakura_network::PeerSource;
use zakura_state as zs;

use crate::BoxError;

const CHECK_INTERVAL: Duration = Duration::from_secs(2);
const CHECK_TIMEOUT: Duration = Duration::from_secs(5);
const DEFERRED_LIFETIME: Duration = Duration::from_secs(8 * 60);
const RETRY_LIMIT: usize = 3;
const CHECK_CONCURRENCY: usize = 4;

/// A refused admission. Its caller must release its active task and block body.
#[derive(Clone, Debug, thiserror::Error)]
#[error("block {hash} deferred until parent {parent} is usable")]
pub struct Deferred {
    pub hash: block::Hash,
    pub parent: block::Hash,
    pub height: Height,
    pub routing_source: Option<PeerSource>,
    pub parent_recovery: bool,
}

#[derive(Debug)]
struct Entry {
    block: Deferred,
    first: Instant,
    next_retry: Instant,
    retries: usize,
    dispatched: bool,
    ready: bool,
    owner: u64,
}

#[derive(Debug)]
struct Charge {
    parent: block::Hash,
    since: Instant,
}

#[derive(Debug)]
struct Accounting {
    capacity: usize,
    generation: u64,
    next_attempt: u64,
    charges: HashMap<u64, Charge>,
    deferred: indexmap::IndexMap<block::Hash, Entry>,
    next_checks: HashMap<block::Hash, Instant>,
    peak: usize,
    band: usize,
    rediscover: bool,
}

#[derive(Debug)]
struct Inner {
    path: &'static str,
    accounting: Mutex<Accounting>,
    changed: Notify,
}

/// Independent admission and deferred-hash accounting for one downloader.
#[derive(Clone, Debug)]
pub(super) struct Budget(Arc<Inner>);

/// Unique attempt ownership; dropping it releases only this attempt's charge.
pub(super) struct Attempt {
    budget: Budget,
    id: u64,
    generation: u64,
    hash: block::Hash,
    transferred: bool,
    outcome: &'static str,
}

impl Budget {
    pub fn new(path: &'static str, capacity: usize) -> Self {
        let budget = Self(Arc::new(Inner {
            path,
            accounting: Mutex::new(Accounting {
                capacity: capacity.max(1),
                generation: 0,
                next_attempt: 0,
                charges: HashMap::new(),
                deferred: indexmap::IndexMap::new(),
                next_checks: HashMap::new(),
                peak: 0,
                band: 0,
                rediscover: false,
            }),
            changed: Notify::new(),
        }));
        budget.publish(&mut budget.lock(), false);
        budget
    }

    fn lock(&self) -> MutexGuard<'_, Accounting> {
        self.0
            .accounting
            .lock()
            .expect("download accounting is only changed by non-panicking operations")
    }

    pub fn limit(capacity: usize) -> usize {
        capacity.saturating_sub(capacity.div_ceil(4))
    }

    fn metric(&self, suffix: &str) -> String {
        format!("{}.missing_parent.{suffix}", self.0.path)
    }

    fn count(&self, name: &str) {
        metrics::counter!(self.metric(name)).increment(1);
    }

    fn publish(&self, state: &mut Accounting, capacity_change: bool) {
        let active = state.charges.len();
        let limit = Self::limit(state.capacity);
        state.peak = state.peak.max(active);
        let band = if limit == 0 {
            0
        } else if active >= limit {
            3
        } else if active.saturating_mul(10) >= limit.saturating_mul(9) {
            2
        } else if active.saturating_mul(5) >= limit.saturating_mul(4) {
            1
        } else {
            0
        };
        if band > state.band {
            if state.band < 1 {
                self.count("near_80.count");
            }
            if band >= 2 && state.band < 2 {
                self.count("near_90.count");
            }
            if band == 3 && !capacity_change {
                self.count("limit_reached.count");
            }
        }
        if capacity_change && active > 0 && active >= limit {
            self.count("capacity_reduction_overage.count");
        }
        state.band = band;
        for (name, value) in [
            ("capacity", state.capacity),
            ("reserved", state.capacity - limit),
            ("limit", limit),
            ("active", active),
            ("headroom", limit.saturating_sub(active)),
            ("peak", state.peak),
            (
                "deferred",
                state.deferred.values().filter(|e| !e.dispatched).count(),
            ),
        ] {
            // Counts and configured capacities are small enough for exact f64 integers.
            metrics::gauge!(self.metric(name)).set(value as f64);
        }
        let oldest = state
            .deferred
            .values()
            .filter(|e| !e.dispatched)
            .map(|e| e.first.elapsed().as_secs_f64())
            .fold(0.0, f64::max);
        metrics::gauge!(self.metric("deferred.oldest_age_seconds")).set(oldest);
        // A new process exposes zero counters even before its first admission.
        for name in [
            "limit_reached.count",
            "capacity_deferred.count",
            "near_80.count",
            "near_90.count",
            "capacity_reduction_overage.count",
            "available.count",
            "admitted.count",
            "read_failed.count",
            "inconclusive.count",
            "expired.count",
            "overflow.count",
            "retry_limit.count",
            "retried.count",
        ] {
            metrics::counter!(self.metric(name)).increment(0);
        }
    }

    pub fn set_capacity(&self, capacity: usize) {
        let mut state = self.lock();
        if state.capacity == capacity.max(1) {
            return;
        }
        state.capacity = capacity.max(1);
        while state.deferred.len() > state.capacity {
            if let Some(index) = state
                .deferred
                .iter()
                .rposition(|(_, entry)| !entry.dispatched)
            {
                state.deferred.shift_remove_index(index);
                state.rediscover = true;
                self.count("overflow.count");
            } else {
                break;
            }
        }
        self.publish(&mut state, true);
        self.0.changed.notify_one();
    }

    pub fn attempt(&self, hash: block::Hash) -> Attempt {
        let mut state = self.lock();
        state.next_attempt = state
            .next_attempt
            .checked_add(1)
            .expect("a downloader cannot start u64::MAX attempts in its lifetime");
        let id = state.next_attempt;
        if let Some(entry) = state.deferred.get_mut(&hash) {
            if entry.dispatched {
                self.count("retried.count");
            }
            entry.owner = id;
        }
        Attempt {
            budget: self.clone(),
            id,
            generation: state.generation,
            hash,
            transferred: false,
            outcome: "cancelled",
        }
    }

    pub fn is_deferred(&self, hash: &block::Hash) -> bool {
        self.lock()
            .deferred
            .get(hash)
            .is_some_and(|entry| !entry.dispatched)
    }

    fn release(&self, id: u64, reason: &'static str) {
        let mut state = self.lock();
        if let Some(charge) = state.charges.remove(&id) {
            metrics::histogram!(self.metric("charge.duration_seconds"), "reason" => reason)
                .record(charge.since.elapsed().as_secs_f64());
            self.publish(&mut state, false);
        }
    }

    /// Returns ready metadata only; actual dispatch must still obey the active window.
    pub fn take_ready(&self, slots: usize) -> Vec<Deferred> {
        let mut state = self.lock();
        self.expire(&mut state);
        let mut ready = Vec::new();
        for entry in state.deferred.values_mut() {
            if ready.len() >= slots {
                break;
            }
            if entry.ready && !entry.dispatched && entry.next_retry <= Instant::now() {
                entry.dispatched = true;
                entry.ready = false;
                entry.next_retry = Instant::now() + CHECK_INTERVAL;
                entry.retries += 1;
                ready.push(entry.block.clone());
            }
        }
        self.publish(&mut state, false);
        ready
    }

    fn expire(&self, state: &mut Accounting) {
        let now = Instant::now();
        state.deferred.retain(|_, entry| {
            if entry.dispatched {
                return true;
            }
            let reason = if now.duration_since(entry.first) >= DEFERRED_LIFETIME {
                Some("expired.count")
            } else if entry.retries >= RETRY_LIMIT {
                Some("retry_limit.count")
            } else {
                None
            };
            if let Some(reason) = reason {
                self.count(reason);
                metrics::histogram!(self.metric("deferred.duration_seconds"), "reason" => reason)
                    .record(now.duration_since(entry.first).as_secs_f64());
                state.rediscover = true;
                false
            } else {
                true
            }
        });
    }

    pub fn take_rediscovery(&self) -> bool {
        std::mem::take(&mut self.lock().rediscover)
    }

    pub fn dispatch_refused(&self, hash: &block::Hash) {
        if let Some(entry) = self.lock().deferred.get_mut(hash) {
            entry.dispatched = false;
            entry.ready = true;
            entry.retries = entry.retries.saturating_sub(1);
        }
    }

    pub fn has_pending(&self) -> bool {
        let state = self.lock();
        !state.charges.is_empty() || state.deferred.values().any(|entry| !entry.dispatched)
    }

    pub fn reset(&self) {
        let mut state = self.lock();
        state.generation = state
            .generation
            .checked_add(1)
            .expect("a downloader cannot reset u64::MAX rounds in its lifetime");
        state.deferred.clear();
        state.next_checks.clear();
        state.rediscover = false;
        // Running attempts own their charges until cancellation actually drops them.
        self.publish(&mut state, false);
        self.0.changed.notify_one();
    }

    /// Observe a bounded batch without blocking the downloader's completion loop.
    pub async fn observe<S, T>(&self, state: S, mut tip: T)
    where
        S: Service<zs::Request, Response = zs::Response, Error = BoxError> + Clone + Send + 'static,
        S::Future: Send,
        T: ChainTip + Send,
    {
        tip.mark_best_tip_seen();
        let mut force = false;
        let mut tip_open = true;
        loop {
            let parents = {
                let mut inner = self.lock();
                let previous_len = inner.deferred.len();
                self.expire(&mut inner);
                if previous_len != inner.deferred.len() {
                    self.publish(&mut inner, false);
                    return;
                }
                let mut parents: HashSet<_> = inner.charges.values().map(|c| c.parent).collect();
                parents.extend(
                    inner
                        .deferred
                        .values()
                        .filter(|e| !e.dispatched)
                        .map(|e| e.block.parent),
                );
                inner
                    .next_checks
                    .retain(|parent, _| parents.contains(parent));
                let now = Instant::now();
                let mut due: Vec<_> = parents
                    .into_iter()
                    .filter(|parent| {
                        force
                            || inner
                                .next_checks
                                .get(parent)
                                .is_none_or(|next| *next <= now)
                    })
                    .collect();
                due.sort_by_key(|parent| inner.next_checks.get(parent).copied());
                due.truncate(CHECK_CONCURRENCY);
                for parent in &due {
                    inner.next_checks.insert(*parent, now + CHECK_INTERVAL);
                }
                self.publish(&mut inner, false);
                due
            };
            if !parents.is_empty() {
                let checks = parents
                    .into_iter()
                    .map(|parent| {
                        let state = state.clone();
                        async move { (parent, parent_available(state, parent).await) }
                    })
                    .collect::<Vec<_>>();
                let results = join_all(checks).await;
                for (parent, result) in results {
                    if matches!(result, Ok(zs::ParentInputs::Available)) {
                        self.parent_ready(parent);
                    } else if result.is_err() {
                        self.count("read_failed.count");
                    }
                }
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(CHECK_INTERVAL) => { force = false; }
                _ = self.0.changed.notified() => { force = false; }
                changed = tip.best_tip_changed(), if tip_open => {
                    tip_open = changed.is_ok();
                    force = tip_open;
                }
            }
        }
    }

    fn parent_ready(&self, parent: block::Hash) {
        let ids: Vec<_> = self
            .lock()
            .charges
            .iter()
            .filter(|(_, c)| c.parent == parent)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.release(id, "parent_available");
        }
        let mut state = self.lock();
        for entry in state
            .deferred
            .values_mut()
            .filter(|e| e.block.parent == parent)
        {
            entry.ready = true;
        }
    }
}

/// Confirm usable ancestry through the state boundary, never through inventory.
async fn parent_available<S>(state: S, parent: block::Hash) -> Result<zs::ParentInputs, BoxError>
where
    S: Service<zs::Request, Response = zs::Response, Error = BoxError> + Clone + Send + 'static,
    S::Future: Send,
{
    match timeout(
        CHECK_TIMEOUT,
        state.oneshot(zs::Request::CheckParentInputs {
            parent,
            outpoints: Arc::from([]),
        }),
    )
    .await??
    {
        zs::Response::ParentInputs(result) => Ok(result),
        _ => unreachable!("CheckParentInputs returns ParentInputs"),
    }
}

impl Attempt {
    /// Try admission without holding an active task waiting for an accounting slot.
    pub async fn admit<S>(&mut self, state: S, block: Deferred) -> Result<(), Deferred>
    where
        S: Service<zs::Request, Response = zs::Response, Error = BoxError> + Clone + Send + 'static,
        S::Future: Send,
    {
        let result = parent_available(state, block.parent).await;
        if matches!(result, Ok(zs::ParentInputs::Available)) {
            self.budget.count("available.count");
            return Ok(());
        }
        if result.is_err() {
            self.budget.count("read_failed.count");
        } else {
            if matches!(result, Ok(zs::ParentInputs::Inconclusive)) {
                self.budget.count("inconclusive.count");
            }
            let mut inner = self.budget.lock();
            if self.generation != inner.generation {
                return Err(block);
            }
            if inner.charges.len() < Budget::limit(inner.capacity) {
                inner.charges.insert(
                    self.id,
                    Charge {
                        parent: block.parent,
                        since: Instant::now(),
                    },
                );
                self.budget.count("admitted.count");
                self.budget.publish(&mut inner, false);
                self.budget.0.changed.notify_one();
                return Ok(());
            }
            self.budget.count("capacity_deferred.count");
        }
        self.defer(block.clone());
        Err(block)
    }

    fn defer(&mut self, block: Deferred) {
        let mut inner = self.budget.lock();
        if self.generation != inner.generation {
            return;
        }
        if let Some(entry) = inner.deferred.get_mut(&self.hash) {
            entry.dispatched = false;
            entry.ready = false;
            entry.owner = self.id;
        } else if inner.deferred.len() < inner.capacity {
            inner.deferred.insert(
                self.hash,
                Entry {
                    block,
                    first: Instant::now(),
                    next_retry: Instant::now(),
                    retries: 0,
                    dispatched: false,
                    ready: false,
                    owner: self.id,
                },
            );
        } else {
            inner.rediscover = true;
            self.budget.count("overflow.count");
        }
        self.transferred = true;
        self.budget.expire(&mut inner);
        self.budget.publish(&mut inner, false);
        self.budget.0.changed.notify_one();
    }

    pub fn completed(&mut self, success: bool) {
        self.outcome = if success { "completed" } else { "failed" };
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        self.budget.release(self.id, self.outcome);
        if !self.transferred {
            let mut state = self.budget.lock();
            if state
                .deferred
                .get(&self.hash)
                .is_some_and(|e| e.owner == self.id)
            {
                state.deferred.shift_remove(&self.hash);
            }
            self.budget.publish(&mut state, false);
        }
    }
}

#[cfg(test)]
mod tests;
