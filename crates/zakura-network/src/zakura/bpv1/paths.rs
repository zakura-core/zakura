//! Pure path-estimate machine (`PeerPaths`, `decision-12`).
//!
//! `PeerPaths` stores the latest optional `(min_rtt, rate_in, rate_out)`
//! observation for each live peer connection and computes the single quantity
//! every BPv1 routing choice compares: **when a part requested from a peer
//! would arrive at this node**.
//!
//! Path estimates are strictly routing hints — never a peer fault (`bpv1-40`),
//! and entries are created and dropped 1:1 with live connections (`bpv1-65`).

use std::time::Duration;

use zakura_quic::NodeId;
use zakura_rt::{collections::HashMap, time::Instant};

use super::{peer::SLOW_X, wire::PROPAGATION_DEPTH};

/// Fallback bandwidth (100 KB/s) used when no peer in the table has a measured
/// bandwidth (`decision-3`).
pub const DEFAULT_UNMEASURED_RATE_BPS: u64 = 100_000;

/// One peer's path as IO measured it.
///
/// Every field is optional: a missing field falls back to the median over
/// currently measured peers, or `(0 ms, 100 KB/s)` when no peer is measured.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct PathEstimate {
    /// Minimum observed round-trip time for this peer.
    pub min_rtt: Option<Duration>,
    /// Bytes per second this peer delivers to this node.
    pub rate_in: Option<u64>,
    /// Bytes per second this node delivers to this peer.
    pub rate_out: Option<u64>,
}

/// Which direction of a path an estimate describes.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PathDirection {
    /// Data a peer sends to this node (`rate_in`): pulls.
    Pull,
    /// Data this node sends to a peer (`rate_out`): proposer seeds.
    Seed,
}

/// One arrival estimate and the path terms it used (`decision-12`).
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Arrival {
    /// When the last requested byte would arrive.
    pub at: Instant,
    /// RTT used for the estimate.
    pub rtt: Duration,
    /// Bandwidth used for the estimate, in bytes per second (always `>= 1`).
    pub rate_bps: u64,
}

/// Medians over measured peers, recomputed whenever an entry changes.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Medians {
    rtt: Duration,
    rate_in: u64,
    rate_out: u64,
}

impl Default for Medians {
    fn default() -> Self {
        Self {
            rtt: Duration::ZERO,
            rate_in: DEFAULT_UNMEASURED_RATE_BPS,
            rate_out: DEFAULT_UNMEASURED_RATE_BPS,
        }
    }
}

/// How long a delivery-rate sample counts toward a peer's bandwidth: the
/// time `PROPAGATION_DEPTH` blocks take at the target spacing, so the
/// estimate spans many blocks (decision-21).
pub const RATE_WINDOW: Duration = Duration::from_secs(
    PROPAGATION_DEPTH as u64 * zakura_chain::parameters::POST_BLOSSOM_POW_TARGET_SPACING as u64,
);

/// One delivery-rate sample from a `Want` that received parts (decision-21).
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RateSample {
    /// Bytes of the parts attributed to the `Want`.
    pub bytes: u64,
    /// When this node queued the `Want`.
    pub sent_at: Instant,
    /// When the first attributed part arrived.
    pub first_at: Instant,
    /// When the last attributed part arrived.
    pub last_at: Instant,
}

/// A windowed max over delivery-rate samples in two `RATE_WINDOW` slots, so
/// a sample counts for one to two windows in constant space.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
struct RateWindow {
    current_from: Option<Instant>,
    current: u64,
    previous: u64,
}

impl RateWindow {
    fn record(&mut self, at: Instant, rate: u64) {
        match self.current_from {
            Some(from) if at.saturating_duration_since(from) < RATE_WINDOW => {}
            Some(from) if at.saturating_duration_since(from) < RATE_WINDOW * 2 => {
                self.previous = self.current;
                self.current = 0;
                self.current_from = Some(from + RATE_WINDOW);
            }
            _ => {
                self.previous = 0;
                self.current = 0;
                self.current_from = Some(at);
            }
        }
        self.current = self.current.max(rate);
    }

    fn max(&self) -> u64 {
        self.current.max(self.previous)
    }
}

/// Pure per-node table of peer path estimates.
#[derive(Clone, Debug, Default)]
pub struct PeerPaths {
    entries: HashMap<NodeId, PathEstimate>,
    /// Per-response delivery-rate samples, per live connection (decision-21).
    samples: HashMap<NodeId, RateWindow>,
    medians: Medians,
}

impl PeerPaths {
    /// Creates an empty path table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records or replaces a live connection's path estimate (`bpv1-65`).
    pub fn observe(&mut self, peer: NodeId, estimate: PathEstimate) {
        let normalized = PathEstimate {
            min_rtt: estimate.min_rtt,
            rate_in: estimate.rate_in.filter(|&r| r > 0),
            rate_out: estimate.rate_out.filter(|&r| r > 0),
        };
        self.entries.insert(peer, normalized);
        self.recompute_medians();
    }

    /// Records one delivery-rate sample from `peer` (decision-21, 03 §6.4):
    /// `transfer = max(t_last − t_sent − min_rtt, t_last − t_first)` and
    /// `rate = bytes / transfer`. Returns the sample's rate. A transfer that
    /// rounds to zero counts as one millisecond.
    pub fn record_sample(&mut self, peer: NodeId, sample: RateSample) -> u64 {
        let min_rtt = self.resolve(&peer, PathDirection::Pull).0;
        let transfer = sample
            .last_at
            .saturating_duration_since(sample.sent_at)
            .saturating_sub(min_rtt)
            .max(sample.last_at.saturating_duration_since(sample.first_at))
            .max(Duration::from_millis(1));
        let rate = u128::from(sample.bytes).saturating_mul(1_000_000_000) / transfer.as_nanos();
        let rate = u64::try_from(rate).unwrap_or(u64::MAX);
        self.samples
            .entry(peer)
            .or_default()
            .record(sample.last_at, rate);
        self.recompute_medians();
        rate
    }

    /// `peer`'s bandwidth to this node: the windowed max of its delivery-rate
    /// samples and the transport's `rate_in` (decision-21).
    fn bw_in(&self, peer: &NodeId) -> Option<u64> {
        let sampled = self
            .samples
            .get(peer)
            .map(RateWindow::max)
            .filter(|&r| r > 0);
        let rate_in = self.entries.get(peer).and_then(|e| e.rate_in);
        rate_in.max(sampled)
    }

    /// Drops the path entry when a connection ends (`bpv1-65`).
    pub fn remove(&mut self, peer: &NodeId) -> bool {
        self.samples.remove(peer);
        let removed = self.entries.remove(peer).is_some();
        if removed {
            self.recompute_medians();
        }
        removed
    }

    /// Number of tracked live connections (`path.entries` gauge).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` when no peers are tracked.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the raw estimate stored for `peer`, if any.
    pub fn get(&self, peer: &NodeId) -> Option<PathEstimate> {
        self.entries.get(peer).copied()
    }

    /// Counts peers lacking either `min_rtt` or `rate_in` (`path.unmeasured_peers`).
    pub fn unmeasured_pull_peers(&self) -> usize {
        self.entries
            .values()
            .filter(|e| e.min_rtt.is_none() || e.rate_in.is_none())
            .count()
    }

    /// Median `min_rtt` across measured peers, or `Duration::ZERO` if none.
    pub fn median_rtt(&self) -> Duration {
        self.medians.rtt
    }

    /// Median bandwidth in `direction` across measured peers, or 100 KB/s if none.
    pub fn median_rate(&self, direction: PathDirection) -> u64 {
        match direction {
            PathDirection::Pull => self.medians.rate_in,
            PathDirection::Seed => self.medians.rate_out,
        }
    }

    /// Estimates when `bytes` sent through `peer` in `direction` would arrive,
    /// behind the `ahead_bytes` the peer already owes:
    /// `now + rtt + (ahead + bytes) / bw`.
    fn arrival(
        &self,
        peer: &NodeId,
        direction: PathDirection,
        ahead_bytes: u64,
        bytes: u64,
        now: Instant,
    ) -> Arrival {
        let (rtt, rate_bps) = self.resolve(peer, direction);
        Arrival {
            at: now + arrival_duration(rtt, ahead_bytes.saturating_add(bytes), rate_bps),
            rtt,
            rate_bps,
        }
    }

    /// When `bytes` pulled from `peer` behind `ahead_bytes` would arrive.
    pub fn pull_arrival(
        &self,
        peer: &NodeId,
        ahead_bytes: u64,
        bytes: u64,
        now: Instant,
    ) -> Arrival {
        self.arrival(peer, PathDirection::Pull, ahead_bytes, bytes, now)
    }

    /// How long `bytes` this node pushes to `peer` take to arrive:
    /// `rtt + bytes / rate_out`.
    pub fn seed_time(&self, peer: &NodeId, bytes: u64) -> Duration {
        let (rtt, rate_bps) = self.resolve(peer, PathDirection::Seed);
        arrival_duration(rtt, bytes, rate_bps)
    }

    /// The rate at which `holder` pushes one copy of each part to its
    /// slowest peer, `push_rate(H)`. This node cannot see the holder's
    /// paths, so the holder's bandwidth to this node stands in, or the median
    /// when unmeasured.
    pub fn push_rate(&self, holder: &NodeId) -> u64 {
        self.resolve(holder, PathDirection::Pull).1
    }

    /// How long until `holder` has pushed `pushed_bytes` to its slowest peer,
    /// once this node seeds it `seed_bytes`:
    /// `seed_arrival(H) + rtt(H) + pushed_bytes / push_rate(H)`
    /// (decision-19). With all of the holder's pushed
    /// bytes this is `finish(H)`; with those through one part, the part's
    /// `push_arrival`.
    pub fn push_arrival(&self, holder: &NodeId, seed_bytes: u64, pushed_bytes: u64) -> Duration {
        let (rtt, push_rate) = self.resolve(holder, PathDirection::Pull);
        self.seed_time(holder, seed_bytes)
            .saturating_add(arrival_duration(rtt, pushed_bytes, push_rate))
    }

    /// The pull arrival of one `part_bytes` part from `peer`, or `None` when
    /// committing it would exceed the pull cap
    /// `max(2 parts, 2 × bw × rtt)` (decision-21).
    pub fn arrival_with_room(
        &self,
        peer: &NodeId,
        ahead_bytes: u64,
        part_bytes: u64,
        now: Instant,
    ) -> Option<Arrival> {
        let arrival = self.arrival(peer, PathDirection::Pull, ahead_bytes, part_bytes, now);
        let cap = part_bytes
            .saturating_mul(2)
            .max(transfer_bytes(arrival.rtt, arrival.rate_bps).saturating_mul(2));
        (ahead_bytes.saturating_add(part_bytes) <= cap).then_some(arrival)
    }

    /// Filters the proposer's neighbours to those whose seed arrival for
    /// `share_bytes` (using `rate_out`) is within `SLOW_X` × the median across
    /// `candidates` (decision-19).
    ///
    /// The fastest candidate always passes, because the median is at least the minimum.
    pub fn eligible_seed_peers(&self, candidates: &[NodeId], share_bytes: u64) -> Vec<NodeId> {
        let scored: Vec<(NodeId, u128)> = candidates
            .iter()
            .map(|&peer| (peer, self.seed_time(&peer, share_bytes).as_nanos()))
            .collect();
        let mut durations: Vec<u128> = scored.iter().map(|&(_, ns)| ns).collect();
        let Some(median_ns) = median(&mut durations) else {
            return Vec::new();
        };
        let limit_ns = median_ns.saturating_mul(u128::from(SLOW_X));
        scored
            .into_iter()
            .filter(|&(_, ns)| ns <= limit_ns)
            .map(|(peer, _)| peer)
            .collect()
    }

    /// `(rtt, bandwidth)` for `peer` in `direction`; a missing term falls
    /// back to the median over measured peers.
    fn resolve(&self, peer: &NodeId, direction: PathDirection) -> (Duration, u64) {
        let entry = self.entries.get(peer).copied().unwrap_or_default();
        let rate = match direction {
            PathDirection::Pull => self.bw_in(peer),
            PathDirection::Seed => entry.rate_out,
        };
        (
            entry.min_rtt.unwrap_or(self.medians.rtt),
            rate.unwrap_or(self.median_rate(direction)).max(1),
        )
    }

    fn recompute_medians(&mut self) {
        let mut rtts: Vec<u128> = self
            .entries
            .values()
            .filter_map(|e| e.min_rtt.map(|d| d.as_nanos()))
            .collect();
        let mut rates_in: Vec<u128> = self
            .entries
            .keys()
            .chain(
                self.samples
                    .keys()
                    .filter(|p| !self.entries.contains_key(*p)),
            )
            .filter_map(|peer| self.bw_in(peer).map(u128::from))
            .collect();
        let mut rates_out: Vec<u128> = self
            .entries
            .values()
            .filter_map(|e| e.rate_out.map(u128::from))
            .collect();
        let defaults = Medians::default();
        // A median of `u64` values fits in `u64`.
        let to_u64 = |v: u128| u64::try_from(v).unwrap_or(u64::MAX);
        self.medians = Medians {
            rtt: median(&mut rtts).map_or(defaults.rtt, |ns| Duration::from_nanos(to_u64(ns))),
            rate_in: median(&mut rates_in).map_or(defaults.rate_in, to_u64),
            rate_out: median(&mut rates_out).map_or(defaults.rate_out, to_u64),
        };
    }
}

/// Bytes that `rate_bps` moves in `duration`, saturating.
pub fn transfer_bytes(duration: Duration, rate_bps: u64) -> u64 {
    let bytes = duration.as_nanos().saturating_mul(u128::from(rate_bps)) / 1_000_000_000;
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

/// Computes `rtt + ceil(bytes / rate_bps)` in nanoseconds without overflow.
pub fn arrival_duration(rtt: Duration, bytes: u64, rate_bps: u64) -> Duration {
    let bw = u128::from(rate_bps.max(1));
    let transfer_ns = u128::from(bytes).saturating_mul(1_000_000_000).div_ceil(bw);
    let total_ns = rtt.as_nanos().saturating_add(transfer_ns);
    Duration::from_nanos(u64::try_from(total_ns).unwrap_or(u64::MAX))
}

fn median(vals: &mut [u128]) -> Option<u128> {
    if vals.is_empty() {
        return None;
    }
    vals.sort_unstable();
    let mid = vals.len() / 2;
    if vals.len().is_multiple_of(2) {
        Some(vals[mid - 1] / 2 + vals[mid] / 2 + (vals[mid - 1] % 2 + vals[mid] % 2) / 2)
    } else {
        Some(vals[mid])
    }
}
