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
//!   dial seeds like any other candidate, up to half its ordinary slots.
//! - At the floor, the candidate dialer stops dialing seeds
//!   ([`SeedDialPolicy`]).
//! - Once [`BOOTSTRAP_RELEASE_HOSTS`] hosts have settled, this dialer closes
//!   the seed connections that discovery opened. A pin, an upgrade hand-off
//!   or a seed's own connection keeps the lifetime its owner gives it
//!   ([`ConnectionOpener`]).
//!
//! A seed is recognized by what its connection's handshake proved: its
//! configured node ID, or an ID it has now that keeps the configured one
//! (zakura-quic TLS-7). A small network whose nodes are all seeds never
//! reaches the floor, so its seeds stay connected as before.
//!
//! [`BootstrapPlan`] owns the policy. [`run_bootstrap_dialer`] does the IO.

use std::{mem, net::IpAddr, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use zakura_node_services::task::spawn_erased_named;
use zakura_quic::{NodeAddr, NodeId};
use zakura_rt::{collections::HashSet, sync::oneshot, task::AbortOnDropHandle, time::Instant};

use super::redial::{score_dial, DialResult, ZAKURA_REDIAL_HEALTHY_CONNECTION};
use crate::zakura::{
    handler::{ConnectionOpener, OutboundPeer},
    ZakuraEndpoint, ZakuraLocalLimits, ZakuraPeerId, DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF,
    DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF, ZAKURA_ALPN_MISMATCH_BACKOFF,
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

/// What the candidate dialer may do with seeds. The bootstrap dialer sets it
/// on the endpoint every check.
#[derive(Debug, Default)]
pub(crate) struct SeedDialPolicy {
    /// Every node ID a seed is known by: the configured one, the one a dial
    /// to it answered with, and the one a connection that proved it has.
    pub(crate) ids: HashSet<NodeId>,
    /// The node has [`BOOTSTRAP_DIAL_FLOOR`] hosts of its own, so it dials
    /// no seed.
    pub(crate) excluded: bool,
}

impl SeedDialPolicy {
    /// The most seed connections the candidate dialer lets this node have,
    /// given its `ordinary_cap` slots: none once seeds are excluded, and
    /// otherwise half, so seeds can't fill the slots the node's own peers
    /// need to reach the floor.
    pub(crate) fn seed_connection_cap(&self, ordinary_cap: usize) -> usize {
        if self.excluded {
            0
        } else {
            ordinary_cap / 2
        }
    }
}

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
    /// Counts the hosts of the outbound non-seed `peers` at `now`. A peer
    /// whose IP isn't known counts as a host of its own.
    pub(super) fn of<'a>(peers: impl IntoIterator<Item = &'a OutboundPeer>, now: Instant) -> Self {
        #[derive(Clone, Copy, Eq, Hash, PartialEq)]
        enum Host<'a> {
            Ip(IpAddr),
            Peer(&'a ZakuraPeerId),
        }
        let mut hosts = HashSet::new();
        let mut settled = HashSet::new();
        for peer in peers {
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
    /// The ID a connection that proved the configured one registered under,
    /// once the seed has added keys.
    known_as: Option<NodeId>,
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
                known_as: None,
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

    /// Every ID seed `index` is known by, given `current(id)`, the ID a dial
    /// to `id` answered with.
    pub(super) fn ids_of(
        &self,
        index: usize,
        current: impl Fn(NodeId) -> NodeId,
    ) -> impl Iterator<Item = NodeId> {
        let seed = &self.seeds[index];
        let configured = seed.node_addr.id;
        let answered = Some(current(configured)).filter(|id| *id != configured);
        [Some(configured), answered, seed.known_as]
            .into_iter()
            .flatten()
    }

    /// Returns which seed each of the `outbound` peers is, if any. A peer is
    /// seed `index` when it is registered under an ID that seed is known by,
    /// or its handshake proved the seed's configured ID. In that last case
    /// the plan remembers the ID it registered under.
    pub(super) fn match_peers(
        &mut self,
        outbound: &[OutboundPeer],
        current: impl Fn(NodeId) -> NodeId,
    ) -> Vec<Option<usize>> {
        outbound
            .iter()
            .map(|peer| {
                let node_id = peer.peer_id.node_id()?;
                let index = (0..self.seeds.len()).find(|&index| {
                    peer.earlier.contains(&self.seeds[index].node_addr.id)
                        || self.ids_of(index, &current).any(|id| id == node_id)
                })?;
                let seed = &mut self.seeds[index];
                if node_id != seed.node_addr.id {
                    seed.known_as = Some(node_id);
                }
                Some(index)
            })
            .collect()
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
/// reads this node's outbound peers, sets the [`SeedDialPolicy`], closes the
/// seed connections discovery opened once enough hosts have settled, and
/// starts the dials [`BootstrapPlan::dials`] picks. A seed found under a new
/// ID gets the operator warning of [`super::dialer::warn_when_moved`].
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
        let current = |id: NodeId| moved.borrow().current(id);
        let outbound = supervisor.outbound_peers().await;
        let matched = plan.match_peers(&outbound, current);
        let coverage = PeerCoverage::of(
            outbound
                .iter()
                .zip(&matched)
                .filter(|(_, seed)| seed.is_none())
                .map(|(peer, _)| peer),
            now,
        );
        let seed_count = plan.configured_ids().count();
        endpoint.set_seed_dial_policy(SeedDialPolicy {
            ids: (0..seed_count)
                .flat_map(|index| plan.ids_of(index, current))
                .collect(),
            excluded: coverage.excludes_seeds(),
        });

        if coverage.releases_seeds() {
            let held = endpoint.held_places();
            for (peer, index) in outbound.iter().zip(&matched) {
                let Some(index) = *index else {
                    continue;
                };
                // A seed the node prefers as a mining gateway keeps its place.
                let preferred = std::iter::once(peer.peer_id.as_bytes())
                    .chain(peer.earlier.iter().map(|id| id.as_bytes().as_slice()))
                    .any(|id| held.contains_bytes(id));
                if peer.opener != ConnectionOpener::Discovery || preferred {
                    continue;
                }
                if supervisor.disconnect_discovery_peer(&peer.peer_id).await {
                    plan.released(index);
                    metrics::counter!("zakura.p2p.bootstrap.released").increment(1);
                }
            }
        }
        let registered: Vec<bool> = (0..seed_count)
            .map(|index| {
                plan.ids_of(index, current)
                    .any(|id| supervisor.is_registered(&ZakuraPeerId::from_node_id(id)))
            })
            .collect();
        for (index, node_addr) in plan.dials(coverage, |index| registered[index], now) {
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
