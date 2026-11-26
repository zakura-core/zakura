//! The bootstrap dialer: keeps a node's configured bootstrap peers (seeds)
//! for finding its first peers, and lets them go once it has its own.
//!
//! A seed connection used to last for the node's whole run, so every node
//! held one inbound slot on every seed, and the seeds filled once the network
//! outgrew their inbound cap. Like Bitcoin Core's address-fetch connections, a
//! seed connection now lasts only until discovery has found other peers:
//!
//! - Below [`BOOTSTRAP_DIAL_FLOOR`] outbound non-seed hosts, this dialer dials
//!   up to [`MAX_BOOTSTRAP_CONNECTIONS`] seeds with a short backoff, so a
//!   node whose seeds were down recovers quickly, and the candidate dialer may
//!   dial seeds like any other candidate.
//! - At the floor, the candidate dialer stops dialing seeds
//!   ([`ZakuraEndpoint::set_excluded_seed_ids`]).
//! - Once [`BOOTSTRAP_RELEASE_HOSTS`] hosts have settled, this dialer closes
//!   every connection this node opened to a seed, whichever dialer opened it,
//!   and stops a legacy-to-Zakura upgrade dial to it so it doesn't reconnect.
//!
//! A small network whose nodes are all seeds never reaches the floor, so its
//! seeds stay connected as before.
//!
//! [`BootstrapPlan`] owns the policy. [`run_bootstrap_dialer`] does the IO.

use std::{mem, net::IpAddr, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use zakura_node_services::task::spawn_erased_named;
use zakura_quic::{NodeAddr, NodeId};
use zakura_rt::{collections::HashSet, sync::oneshot, task::AbortOnDropHandle, time::Instant};

use super::redial::{score_dial, DialResult, ZAKURA_REDIAL_HEALTHY_CONNECTION};
use crate::zakura::{
    handler::OutboundPeer, ZakuraEndpoint, ZakuraLocalLimits, ZakuraPeerId,
    DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF, DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF,
    ZAKURA_ALPN_MISMATCH_BACKOFF,
};

#[cfg(all(test, not(zakura_sim)))]
mod tests;

/// Below this many outbound non-seed hosts, the node dials seeds. Bitcoin Core
/// skips its DNS seeds at the same count of outbound peers
/// (`SEED_OUTBOUND_CONNECTION_THRESHOLD`).
pub(super) const BOOTSTRAP_DIAL_FLOOR: usize = 2;

/// Once this many outbound non-seed hosts have each kept a peer registered for
/// [`ZAKURA_REDIAL_HEALTHY_CONNECTION`], the node closes the connections it
/// opened to seeds. The gap above [`BOOTSTRAP_DIAL_FLOOR`] keeps brief churn
/// from dialing a seed again.
pub(super) const BOOTSTRAP_RELEASE_HOSTS: usize = 4;

/// The most seeds this dialer has connected or is dialing at once. A second
/// seed's sample keeps one dishonest seed from choosing all of a new node's
/// first peers.
pub(super) const MAX_BOOTSTRAP_CONNECTIONS: usize = 2;

/// How often the bootstrap dialer rechecks this node's peers.
const BOOTSTRAP_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// How many hosts back this node's outbound non-seed peers.
///
/// Hosts are [`admission_host`](crate::zakura::handler)s, so peers on one IPv4
/// address or IPv6 /64 count once.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct PeerCoverage {
    /// Hosts with a registered outbound peer.
    hosts: usize,
    /// Hosts with an outbound peer registered for at least
    /// [`ZAKURA_REDIAL_HEALTHY_CONNECTION`].
    settled_hosts: usize,
}

impl PeerCoverage {
    /// Counts the hosts of the `outbound` peers outside `seeds` at `now`. A
    /// peer whose IP isn't known counts as a host of its own.
    pub(super) fn of(
        outbound: &[OutboundPeer],
        seeds: &HashSet<ZakuraPeerId>,
        now: Instant,
    ) -> Self {
        #[derive(Clone, Copy, Eq, Hash, PartialEq)]
        enum Host<'a> {
            Ip(IpAddr),
            Peer(&'a ZakuraPeerId),
        }
        let mut hosts = HashSet::new();
        let mut settled = HashSet::new();
        for peer in outbound
            .iter()
            .filter(|peer| !seeds.contains(&peer.peer_id))
        {
            let host = peer.host.map_or(Host::Peer(&peer.peer_id), Host::Ip);
            if now.saturating_duration_since(peer.registered_at) >= ZAKURA_REDIAL_HEALTHY_CONNECTION
            {
                settled.insert(host);
            }
            hosts.insert(host);
        }
        Self {
            hosts: hosts.len(),
            settled_hosts: settled.len(),
        }
    }

    /// Whether the node has enough peers of its own that the candidate
    /// dialer stops dialing seeds.
    pub(super) fn excludes_seeds(self) -> bool {
        self.hosts >= BOOTSTRAP_DIAL_FLOOR
    }

    /// Whether the node has settled enough peers of its own to close its
    /// seed connections.
    pub(super) fn releases_seeds(self) -> bool {
        self.settled_hosts >= BOOTSTRAP_RELEASE_HOSTS
    }
}

/// One configured seed and the state of this dialer's dials to it.
#[derive(Debug)]
struct Seed {
    node_addr: NodeAddr,
    /// The wait after this seed's next failed dial.
    backoff: Duration,
    /// No dial to this seed starts before this.
    retry_at: Instant,
    /// A dial this dialer started is connecting to or serving this seed.
    dialing: bool,
    /// This node closed the seed's connection, so the dial's result says
    /// nothing about the seed.
    released: bool,
}

/// Which seeds this dialer dials, and how their dials ended.
#[derive(Debug)]
pub(super) struct BootstrapPlan {
    seeds: Vec<Seed>,
    /// Where the next search for a seed to dial starts, so failed seeds
    /// rotate.
    next: usize,
}

impl BootstrapPlan {
    /// Plans dials to `seeds`, in this order, the first allowed at `now`.
    pub(super) fn new(seeds: Vec<NodeAddr>, now: Instant) -> Self {
        let seeds = seeds
            .into_iter()
            .map(|node_addr| Seed {
                node_addr,
                backoff: DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF,
                retry_at: now,
                dialing: false,
                released: false,
            })
            .collect();
        Self { seeds, next: 0 }
    }

    /// The seeds' configured node IDs, in plan order.
    pub(super) fn configured_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.seeds.iter().map(|seed| seed.node_addr.id)
    }

    /// Returns the seeds to dial at `now`, given this node's `coverage` and
    /// whether each seed is `registered` now, and marks them dialing.
    ///
    /// Below [`BOOTSTRAP_DIAL_FLOOR`] hosts, it tops the seeds that are
    /// connected or being dialed up to [`MAX_BOOTSTRAP_CONNECTIONS`], taking
    /// the next ones in order that are out of backoff.
    pub(super) fn dials(
        &mut self,
        coverage: PeerCoverage,
        registered: impl Fn(usize) -> bool,
        now: Instant,
    ) -> Vec<(usize, NodeAddr)> {
        if coverage.excludes_seeds() || self.seeds.is_empty() {
            return Vec::new();
        }
        let mut busy = (0..self.seeds.len())
            .filter(|&index| self.seeds[index].dialing || registered(index))
            .count();
        let mut started = Vec::new();
        for offset in 0..self.seeds.len() {
            if busy >= MAX_BOOTSTRAP_CONNECTIONS {
                break;
            }
            let index = (self.next + offset) % self.seeds.len();
            let seed = &self.seeds[index];
            if seed.dialing || registered(index) || seed.retry_at > now {
                continue;
            }
            self.seeds[index].dialing = true;
            busy += 1;
            started.push((index, self.seeds[index].node_addr.clone()));
        }
        if let Some((last, _)) = started.last() {
            self.next = (last + 1) % self.seeds.len();
        }
        started
    }

    /// Records that this node closed seed `index`'s connection, so a dial of
    /// this dialer's that served it isn't scored as a failure.
    pub(super) fn released(&mut self, index: usize) {
        let seed = &mut self.seeds[index];
        seed.released = seed.dialing;
    }

    /// Records how this dialer's dial to seed `index` ended at `now`.
    ///
    /// A connection this node closed, or one that stayed up for
    /// [`ZAKURA_REDIAL_HEALTHY_CONNECTION`], leaves the seed dialable at
    /// once. A failure waits out a backoff that doubles up to
    /// [`DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF`].
    pub(super) fn finished(&mut self, index: usize, result: DialResult, now: Instant) {
        let seed = &mut self.seeds[index];
        seed.dialing = false;
        if mem::take(&mut seed.released) {
            seed.backoff = DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF;
            seed.retry_at = now;
            return;
        }
        let wait = match result {
            DialResult::Healthy => {
                seed.backoff = DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF;
                Duration::ZERO
            }
            DialResult::LocalLimit => DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF,
            DialResult::Failed => seed.backoff,
            DialResult::AlpnMismatch => seed.backoff.max(ZAKURA_ALPN_MISMATCH_BACKOFF),
        };
        if matches!(result, DialResult::Failed | DialResult::AlpnMismatch) {
            seed.backoff = seed
                .backoff
                .saturating_mul(2)
                .min(DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF);
        }
        seed.retry_at = now + wait;
    }
}

/// Runs the bootstrap dialer for `seeds` until the endpoint shuts down.
///
/// Every [`BOOTSTRAP_CHECK_INTERVAL`], and whenever a seed dial ends, it
/// reads this node's outbound peers, sets which seeds the candidate dialer
/// skips, closes the seed connections once enough hosts have settled, and
/// starts the dials [`BootstrapPlan::dials`] picks. Seeds are matched under
/// the ID each answers with now as well as the configured one (zakura-quic
/// TLS-7), and a seed found under a new ID gets the operator warning of
/// [`super::dialer::warn_when_moved`].
pub(super) async fn run_bootstrap_dialer(
    endpoint: ZakuraEndpoint,
    seeds: Vec<NodeAddr>,
    limits: ZakuraLocalLimits,
) {
    let shutdown = endpoint.background_shutdown_token();
    let supervisor = endpoint.supervisor();
    let moved = supervisor.moved_node_ids();
    let mut warnings: FuturesUnordered<_> = seeds
        .iter()
        .map(|seed| super::dialer::warn_when_moved(supervisor.moved_node_ids(), seed.id))
        .collect();
    let mut plan = BootstrapPlan::new(seeds, Instant::now());
    let mut dials = FuturesUnordered::new();
    loop {
        let now = Instant::now();
        let current: Vec<NodeId> = plan
            .configured_ids()
            .map(|id| moved.borrow().current(id))
            .collect();
        let current_peers: Vec<ZakuraPeerId> = current
            .iter()
            .map(|id| ZakuraPeerId::from_node_id(*id))
            .collect();
        let seed_ids: HashSet<NodeId> = plan
            .configured_ids()
            .chain(current.iter().copied())
            .collect();
        let seed_peers: HashSet<ZakuraPeerId> = seed_ids
            .iter()
            .map(|id| ZakuraPeerId::from_node_id(*id))
            .collect();
        let outbound = supervisor.outbound_peers().await;
        let coverage = PeerCoverage::of(&outbound, &seed_peers, now);

        endpoint.set_excluded_seed_ids(if coverage.excludes_seeds() {
            seed_ids
        } else {
            HashSet::new()
        });
        if coverage.releases_seeds() {
            let held = endpoint.held_places();
            for (index, peer_id) in current_peers.iter().enumerate() {
                // A seed the node prefers as a mining gateway keeps its place.
                if !outbound.iter().any(|peer| &peer.peer_id == peer_id)
                    || held.contains_bytes(peer_id.as_bytes())
                {
                    continue;
                }
                endpoint.cancel_upgrade_native_dial(peer_id);
                if supervisor.disconnect_outbound_peer(peer_id).await {
                    plan.released(index);
                    metrics::counter!("zakura.p2p.bootstrap.released").increment(1);
                }
            }
        }
        let registered = |index: usize| supervisor.is_registered(&current_peers[index]);
        for (index, node_addr) in plan.dials(coverage, registered, now) {
            metrics::counter!("zakura.p2p.bootstrap.dial.started").increment(1);
            dials.push(seed_dial(
                endpoint.clone(),
                node_addr,
                limits.clone(),
                index,
            ));
        }

        zakura_rt::select! {
            biased;
            // Endpoint shutdown cancels this token. It is the only teardown
            // signal, because the dialer holds an endpoint clone.
            _ = shutdown.cancelled() => break,
            ended = dials.next(), if !dials.is_empty() => {
                if let Some((index, result)) = ended {
                    plan.finished(index, result, Instant::now());
                }
            }
            _ = warnings.next(), if !warnings.is_empty() => {}
            _ = zakura_rt::time::sleep(BOOTSTRAP_CHECK_INTERVAL) => {}
        }
    }
}

/// Dials and serves seed `index` at `node_addr` in a task of its own, and
/// returns how the dial ended.
async fn seed_dial(
    endpoint: ZakuraEndpoint,
    node_addr: NodeAddr,
    limits: ZakuraLocalLimits,
    index: usize,
) -> (usize, DialResult) {
    let task = AbortOnDropHandle::new(spawn_erased_named("bootstrap_dial", async move {
        let (registration, registered) = oneshot::channel();
        score_dial(
            super::dialer::native_bootstrap_dial(&endpoint, node_addr, &limits, registration),
            registered,
        )
        .await
    }));
    (index, task.await.unwrap_or(DialResult::Failed))
}
