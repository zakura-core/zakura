//! The bootstrap dialer: keeps a node's configured bootstrap peers (seeds)
//! for finding its first peers, and lets them go once it has its own.
//!
//! A seed connection used to last for the node's whole run, so every node
//! held one inbound slot on every seed, and the seeds filled once the network
//! outgrew their inbound cap. Like Bitcoin Core's address-fetch connections, a
//! seed connection now lasts only until discovery has found other peers:
//!
//! - Only this dialer dials seeds. The candidate dialer skips their IDs
//!   ([`ZakuraEndpoint::set_seed_ids`]).
//! - Below [`BOOTSTRAP_DIAL_FLOOR`] outbound non-seed hosts, this dialer keeps
//!   up to [`MAX_BOOTSTRAP_CONNECTIONS`] seeds connected, with a short backoff
//!   so a node whose seeds were down recovers quickly.
//! - Once [`release_hosts`] outbound non-seed hosts have settled, it closes
//!   the seed connections it opened. Each of its dials serves its connection
//!   under a token of its own ([`NativeDialKind::Bootstrap`]), so a release
//!   closes those and nothing else: a seed's own connection, a pin's or an
//!   upgrade hand-off's stays.
//!
//! A node that finds its own ID among its seeds is a seed itself, as on the
//! default seeds and a fleet that lists all its nodes. It keeps every other
//! seed connected and never closes them, as before, so the seeds stay a full
//! mesh that reconnects after an outage. That costs each seed one inbound
//! slot per other seed.
//!
//! An ordinary node that finds no other peers, such as one in a small network
//! whose nodes list only each other, keeps two seeds connected.
//!
//! [`BootstrapPlan`] owns the policy. [`run_bootstrap_dialer`] does the IO.
//!
//! [`NativeDialKind::Bootstrap`]: crate::zakura::handler::NativeDialKind::Bootstrap

use std::{mem, net::IpAddr, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use zakura_node_services::task::spawn_erased_named;
use zakura_quic::{NodeAddr, NodeId};
use zakura_rt::{
    collections::HashSet,
    sync::{oneshot, CancellationToken},
    task::AbortOnDropHandle,
    time::Instant,
};

use super::protocol::ZakuraDiscoveryHandle;
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

/// The most outbound non-seed hosts a node waits for before it closes its
/// seed connections, see [`release_hosts`]. The gap above
/// [`BOOTSTRAP_DIAL_FLOOR`] keeps brief churn from dialing a seed again.
pub(super) const BOOTSTRAP_RELEASE_HOSTS: usize = 4;

/// The most seeds this node has connected or is dialing at once. A second
/// seed's sample keeps one dishonest seed from choosing all of a new node's
/// first peers.
pub(super) const MAX_BOOTSTRAP_CONNECTIONS: usize = 2;

/// How many outbound non-seed hosts must each keep a peer registered for
/// [`ZAKURA_REDIAL_HEALTHY_CONNECTION`] before the node closes its seed
/// connections, given its `dial_reserve`, the slots inbound peers can't take.
///
/// [`BOOTSTRAP_RELEASE_HOSTS`], unless the reserve left beside the seeds is
/// smaller, as at small caps where inbound peers may fill every other slot.
/// Never below [`BOOTSTRAP_DIAL_FLOOR`], so a release doesn't send the node
/// straight back to dialing seeds.
pub(super) fn release_hosts(dial_reserve: usize) -> usize {
    dial_reserve
        .saturating_sub(MAX_BOOTSTRAP_CONNECTIONS)
        .clamp(BOOTSTRAP_DIAL_FLOOR, BOOTSTRAP_RELEASE_HOSTS)
}

/// How many hosts back this node's outbound non-seed peers.
///
/// Hosts are [`admission_host`](crate::zakura::handler)s, so peers on one IPv4
/// address or IPv6 /64 count once. Only peers this node dialed count, so
/// peers that dial it can't make it drop its seeds.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct PeerCoverage {
    /// Hosts with a registered peer.
    hosts: usize,
    /// Hosts with a peer registered for at least
    /// [`ZAKURA_REDIAL_HEALTHY_CONNECTION`].
    settled_hosts: usize,
    /// When the next peer that hasn't settled yet does.
    next_settled: Option<Instant>,
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
        let mut next_settled: Option<Instant> = None;
        for peer in peers {
            let host = peer.host.map_or(Host::Peer(&peer.peer_id), Host::Ip);
            let settles_at = peer.registered_at + ZAKURA_REDIAL_HEALTHY_CONNECTION;
            if settles_at <= now {
                settled.insert(host);
            } else {
                next_settled = Some(next_settled.map_or(settles_at, |next| next.min(settles_at)));
            }
            hosts.insert(host);
        }
        Self {
            hosts: hosts.len(),
            settled_hosts: settled.len(),
            next_settled,
        }
    }

    /// Whether the node has enough peers of its own that it dials no seed.
    pub(super) fn excludes_seeds(self) -> bool {
        self.hosts >= BOOTSTRAP_DIAL_FLOOR
    }

    /// Whether `needed` hosts have settled, so the node closes its seed
    /// connections.
    pub(super) fn releases_seeds(self, needed: usize) -> bool {
        self.settled_hosts >= needed
    }
}

/// One configured seed and the state of this dialer's dial to it.
#[derive(Debug)]
struct Seed {
    node_addr: NodeAddr,
    /// The wait after this seed's next failed dial.
    backoff: Duration,
    /// No dial to this seed starts before this.
    retry_at: Instant,
    /// The token of the connection a dial of this dialer's is connecting to
    /// or serving, while it runs.
    connection: Option<CancellationToken>,
    /// This node closed the seed's connection, so the dial's result says
    /// nothing about the seed.
    released: bool,
    /// The next dial uses the addresses the seed's signed records advertise
    /// instead of the configured ones. Each failed dial switches sets, so
    /// neither set can crowd the other out.
    records_next: bool,
}

/// Which seeds this dialer dials, and how their dials ended.
#[derive(Debug)]
pub(super) struct BootstrapPlan {
    seeds: Vec<Seed>,
    /// Where the next search for a seed to dial starts, so failed seeds
    /// rotate.
    next: usize,
    /// This node is a seed itself, so it keeps every other seed connected.
    mesh: bool,
}

impl BootstrapPlan {
    /// Plans dials to `seeds`, in this order, the first allowed at `now`. A
    /// `mesh` plan, for a node that is a seed itself, keeps every seed
    /// connected and never closes them.
    pub(super) fn new(seeds: Vec<NodeAddr>, mesh: bool, now: Instant) -> Self {
        let seeds = seeds
            .into_iter()
            .map(|node_addr| Seed {
                node_addr,
                backoff: DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF,
                retry_at: now,
                connection: None,
                released: false,
                records_next: false,
            })
            .collect();
        Self {
            seeds,
            next: 0,
            mesh,
        }
    }

    /// Whether this plan keeps every seed connected, see [`Self::new`].
    pub(super) fn is_mesh(&self) -> bool {
        self.mesh
    }

    /// The seeds' configured node IDs, in plan order.
    pub(super) fn configured_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.seeds.iter().map(|seed| seed.node_addr.id)
    }

    /// Returns the seeds to dial at `now`, given this node's `coverage` and
    /// whether each seed is `registered` now, each with whether to dial the
    /// seed's record addresses ([`Seed::records_next`]) and a new connection
    /// token from `token`, and marks them dialing.
    ///
    /// Below [`BOOTSTRAP_DIAL_FLOOR`] hosts, it tops the seeds that are
    /// connected or being dialed up to [`MAX_BOOTSTRAP_CONNECTIONS`], taking
    /// the next ones in order that are out of backoff. A mesh plan dials
    /// every seed out of backoff that isn't connected, whatever the coverage.
    pub(super) fn dials(
        &mut self,
        coverage: PeerCoverage,
        registered: impl Fn(usize) -> bool,
        now: Instant,
        mut token: impl FnMut() -> CancellationToken,
    ) -> Vec<(usize, NodeAddr, bool, CancellationToken)> {
        if self.seeds.is_empty() || (!self.mesh && coverage.excludes_seeds()) {
            return Vec::new();
        }
        let wanted = if self.mesh {
            self.seeds.len()
        } else {
            MAX_BOOTSTRAP_CONNECTIONS
        };
        let mut busy = (0..self.seeds.len())
            .filter(|&index| self.seeds[index].connection.is_some() || registered(index))
            .count();
        let mut started = Vec::new();
        for offset in 0..self.seeds.len() {
            if busy >= wanted {
                break;
            }
            let index = (self.next + offset) % self.seeds.len();
            let seed = &mut self.seeds[index];
            if seed.connection.is_some() || registered(index) || seed.retry_at > now {
                continue;
            }
            let connection = token();
            seed.connection = Some(connection.clone());
            busy += 1;
            started.push((index, seed.node_addr.clone(), seed.records_next, connection));
        }
        if let Some((last, ..)) = started.last() {
            self.next = (last + 1) % self.seeds.len();
        }
        started
    }

    /// Closes the connections of this dialer's running dials, except to the
    /// seeds `keeps` names by index, and returns how many it closed. Their
    /// dials' results then say nothing about the seeds.
    pub(super) fn release(&mut self, keeps: impl Fn(usize) -> bool) -> usize {
        let mut closed = 0;
        for (index, seed) in self.seeds.iter_mut().enumerate() {
            let Some(connection) = &seed.connection else {
                continue;
            };
            if seed.released || keeps(index) {
                continue;
            }
            connection.cancel();
            seed.released = true;
            closed += 1;
        }
        closed
    }

    /// Whether a dial of this dialer's is running.
    pub(super) fn has_running_dials(&self) -> bool {
        self.seeds.iter().any(|seed| seed.connection.is_some())
    }

    /// When the next idle seed's backoff ends after `now`, if one is waiting.
    pub(super) fn next_retry(&self, now: Instant) -> Option<Instant> {
        self.seeds
            .iter()
            .filter(|seed| seed.connection.is_none() && seed.retry_at > now)
            .map(|seed| seed.retry_at)
            .min()
    }

    /// Records how this dialer's dial to seed `index` ended at `now`.
    ///
    /// A connection this node closed, or one that stayed up for
    /// [`ZAKURA_REDIAL_HEALTHY_CONNECTION`], leaves the seed dialable at
    /// once. A failure waits out a backoff that doubles up to
    /// [`DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF`], and the next dial tries the
    /// other address set.
    pub(super) fn finished(&mut self, index: usize, result: DialResult, now: Instant) {
        let seed = &mut self.seeds[index];
        seed.connection = None;
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
            seed.records_next = !seed.records_next;
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
/// Whenever the node's registrations change, a seed dial ends, a seed's
/// backoff ends or a peer settles, it reads this node's outbound peers,
/// closes its seed connections once enough hosts have settled, and starts the
/// dials [`BootstrapPlan::dials`] picks. A seed is known by its configured ID
/// and the one a dial to it answered with (zakura-quic TLS-7), and a seed
/// found under a new ID gets the operator warning of
/// [`super::dialer::warn_when_moved`].
pub(super) async fn run_bootstrap_dialer(
    endpoint: ZakuraEndpoint,
    discovery: ZakuraDiscoveryHandle,
    seeds: Vec<NodeAddr>,
    mesh: bool,
    limits: ZakuraLocalLimits,
) {
    let shutdown = endpoint.background_shutdown_token();
    let supervisor = endpoint.supervisor();
    let moved = supervisor.moved_node_ids();
    let mut registrations = supervisor.subscribe();
    let needed = release_hosts(
        limits
            .max_connections
            .saturating_sub(limits.max_inbound_connections()),
    );
    let mut warnings: FuturesUnordered<_> = seeds
        .iter()
        .map(|seed| super::dialer::warn_when_moved(supervisor.moved_node_ids(), seed.id))
        .collect();
    let mut plan = BootstrapPlan::new(seeds, mesh, Instant::now());
    let mut dials = FuturesUnordered::new();
    loop {
        let now = Instant::now();
        let ids: Vec<[ZakuraPeerId; 2]> = plan
            .configured_ids()
            .map(|id| [id, moved.borrow().current(id)].map(ZakuraPeerId::from_node_id))
            .collect();
        let seed_peers: HashSet<&ZakuraPeerId> = ids.iter().flatten().collect();
        let outbound = supervisor.outbound_peers().await;
        let coverage = PeerCoverage::of(
            outbound
                .iter()
                .filter(|peer| !seed_peers.contains(&peer.peer_id)),
            now,
        );

        if !plan.is_mesh() && coverage.releases_seeds(needed) {
            let held = endpoint.held_places();
            // A seed the node prefers as a mining gateway keeps its place.
            let closed = plan.release(|index| {
                ids[index]
                    .iter()
                    .any(|id| held.contains_bytes(id.as_bytes()))
            });
            // `closed` counts at most the configured seeds, so it fits u64.
            metrics::counter!("zakura.p2p.bootstrap.released").increment(closed as u64);
        }
        let registered = |index: usize| ids[index].iter().any(|id| supervisor.is_registered(id));
        for (index, node_addr, records, connection) in
            plan.dials(coverage, registered, now, || endpoint.connection_token())
        {
            metrics::counter!("zakura.p2p.bootstrap.dial.started").increment(1);
            let current = moved.borrow().current(node_addr.id);
            dials.push(seed_dial(
                endpoint.clone(),
                records.then(|| (discovery.clone(), current)),
                node_addr,
                limits.clone(),
                connection,
                index,
            ));
        }

        // Below the floor or in a mesh a seed's backoff can end, and while
        // this dialer holds seeds it may release a peer can settle. Nothing
        // else changes without a registration or a dial ending.
        let retry = (plan.is_mesh() || !coverage.excludes_seeds())
            .then(|| plan.next_retry(now))
            .flatten();
        let settle = coverage
            .next_settled
            .filter(|_| !plan.is_mesh() && plan.has_running_dials());
        let wake = retry.into_iter().chain(settle).min();
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
            changed = registrations.changed() => {
                if changed.is_err() {
                    break;
                }
            }
            _ = warnings.next(), if !warnings.is_empty() => {}
            _ = zakura_rt::time::sleep_until(wake.unwrap_or(now)), if wake.is_some() => {}
        }
    }
}

/// Dials and serves seed `index` under `connection` in a task of its own, and
/// returns how the dial ended.
///
/// It dials the configured `node_addr`, or with `records` (the discovery
/// handle and the seed's current ID), the addresses the seed's signed records
/// in the book advertise under its current and configured IDs, so a seed
/// that moved to a new address stays reachable.
/// Only this dialer dials seeds. Record addresses fill only the dial reserve
/// that startup checked the configured ones against, because a dial claims a
/// slot for every attempt at once (zakura-quic ADM-12), and a seed with no
/// usable record is dialed at its configured addresses.
async fn seed_dial(
    endpoint: ZakuraEndpoint,
    records: Option<(ZakuraDiscoveryHandle, NodeId)>,
    mut node_addr: NodeAddr,
    limits: ZakuraLocalLimits,
    connection: CancellationToken,
    index: usize,
) -> (usize, DialResult) {
    let task = AbortOnDropHandle::new(spawn_erased_named("bootstrap_dial", async move {
        if let Some((discovery, current)) = records {
            let mut record = NodeAddr {
                id: node_addr.id,
                direct: discovery.record_addrs(&[current, node_addr.id]).await,
            };
            let reserve = limits.quic_bind_config(Vec::new()).dial_reserve;
            while endpoint.dial_attempt_count(&record) > reserve {
                record.direct.pop();
            }
            if !record.direct.is_empty() {
                node_addr = record;
            }
        }
        let (registration, registered) = oneshot::channel();
        score_dial(
            super::dialer::native_bootstrap_dial(
                &endpoint,
                node_addr,
                &limits,
                connection,
                registration,
            ),
            registered,
        )
        .await
    }));
    (index, task.await.unwrap_or(DialResult::Failed))
}
