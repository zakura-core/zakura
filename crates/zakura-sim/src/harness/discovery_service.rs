//! The discovery node (disc-2.1): production's discovery service behind a
//! real `ZakuraProtocolHandler` on a `zakura-quic` endpoint bound to the
//! run's `SimNet`, through the [`WireEndpoint`] header sync's wire node
//! uses, with sim-5's scripted peers.
//!
//! The node starts discovery as production's endpoint builder does. Its
//! `ZakuraDiscoveryHandle` takes the connected watch from the handler's
//! supervisor, and `DiscoveryService` is the registry's one service. Once
//! the node serves, it seeds the static candidates, then spawns their
//! maintained `bootstrap_dial` tasks and the `discovery_dialer` on its own
//! `ZakuraEndpoint`. All of it runs in the node's root task, so every
//! discovery task is a scheduler task. The handle's verification slot count
//! is fixed, in place of the host's.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use zakura_chain::parameters::Network;
use zakura_network::zakura::{
    testkit::discovery::{
        build_discovery_handle, insert_static_bootstrap_candidates, spawn_native_bootstrap_dialer,
        spawn_native_discovery_dialer,
    },
    DiscoveryService, Service, TransportObserver, ZakuraHandshakeConfig, ZakuraServiceId,
};
use zakura_quic::sim::{SeededInputs, SimNet};

use super::{
    wire_node::{DialReporter, TransportTap, WireEndpoint},
    NetScenario,
};
use crate::{
    peer::{
        key_index, peer_addr, PeerCloses, PeerControl, PeerFailure, PeerRecords, PeerSpec,
        PeerTask, NODE_ADDR,
    },
    Sim,
};

/// The discovery node's own config.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSpec {
    /// The verification slots its handle has, in place of the host's count.
    pub verification_slots: usize,
    /// The scripted peers, by distinct index, that it holds as static
    /// bootstrap candidates and keeps dialing. An index no peer takes names
    /// an address nobody listens on.
    pub static_candidates: Vec<usize>,
}

/// Binds the discovery node to [`NODE_ADDR`] on `net` as
/// [`WireEndpoint::spawn`] does, with discovery alone at production defaults
/// on `network`, and `spec`'s verification slots and static candidates. It
/// reports to `observer`, with failed dials to `dials`.
pub fn spawn_node(
    sim: &mut Sim,
    spec: &NodeSpec,
    network: &Network,
    net: SimNet,
    seed: [u8; 32],
    observer: Arc<dyn TransportObserver>,
    dials: DialReporter,
) {
    let bootstrap_peers = bootstrap_peers(seed, &spec.static_candidates);
    let slots = spec.verification_slots;
    let handshake = ZakuraHandshakeConfig::for_network(network);
    WireEndpoint::spawn(
        sim,
        network,
        net,
        seed,
        observer,
        dials,
        {
            let bootstrap_count = bootstrap_peers.len();
            move |supervisor, limits| {
                let handle = build_discovery_handle(
                    SeededInputs::node_key(seed, 0),
                    // As production passes its listen address. The
                    // address is private, so the node's record drops it.
                    vec![NODE_ADDR],
                    // The node serves discovery alone.
                    vec![ZakuraServiceId::discovery()],
                    &handshake,
                    limits.max_connections,
                    bootstrap_count,
                    supervisor.subscribe(),
                )
                .expect("production's config on the fixture network is valid")
                .with_verification_slots(slots);
                // Production sizes the serving pools for the node's own
                // connection cap, not the default one.
                let service: Arc<dyn Service> = Arc::new(DiscoveryService::with_max_connections(
                    handle.clone(),
                    limits.max_connections,
                ));
                (vec![service], handle)
            }
        },
        |endpoint, limits, handle| async move {
            insert_static_bootstrap_candidates(&handle, &bootstrap_peers).await;
            let mut tasks =
                spawn_native_bootstrap_dialer(endpoint.clone(), bootstrap_peers, limits.clone());
            // A node with no custom services seeks none.
            tasks.push(spawn_native_discovery_dialer(
                endpoint,
                handle,
                limits,
                Vec::new(),
            ));
            tasks
        },
    );
}

/// The bootstrap entries, `<node_id>@<addr>`, for scripted peers `indexes`
/// under `seed`: each peer's key and the address it listens on.
fn bootstrap_peers(seed: [u8; 32], indexes: &[usize]) -> Vec<String> {
    indexes
        .iter()
        .map(|&index| {
            let id = SeededInputs::node_key(seed, key_index(index)).public();
            format!("{id}@{}", peer_addr(index, 0))
        })
        .collect()
}

/// Adds `net`'s network and a transport tap, spawns the discovery node of
/// `spec` on Regtest behind it, then spawns each of sim-5's scripted
/// `peers`, named `peer-<i>`, and adds their action source. The node
/// dials a listening peer only if `spec` holds it as a static candidate.
///
/// # Panics
///
/// If a peer runs the honest script, which needs a header chain.
pub fn spawn(sim: &mut Sim, net: &NetScenario, spec: &NodeSpec, peers: &[PeerSpec]) {
    let network = Network::new_regtest(Default::default());
    let seed = net.seed;
    let net = net.net();
    sim.add_source(Box::new(net.clone()));
    let names: Vec<String> = (0..peers.len())
        .map(|index| format!("peer-{index}"))
        .collect();
    let (observer, tap) = TransportTap::new(sim, "node", net.clone(), names.clone());
    let dials = tap.dials();
    sim.add_source(Box::new(tap));
    spawn_node(sim, spec, &network, net.sim(), seed, observer, dials);
    let failure = PeerFailure::default();
    let records = PeerRecords::default();
    let mut control = PeerControl::new(failure.clone(), records.clone());
    for ((index, peer), name) in peers.iter().enumerate().zip(names) {
        assert!(
            peer.honest.is_none(),
            "a discovery scenario's peers hold no header chain"
        );
        let (readiness, commands) =
            control.add(&name, peer.script.clone(), peer.adversary.is_some());
        let (listening, _) = zakura_rt::sync::watch::channel(false);
        let task = PeerTask {
            name,
            index,
            spec: peer.clone(),
            seed,
            net: net.sim(),
            node: zakura_quic::NodeAddr::with_addrs(
                SeededInputs::node_key(seed, 0).public(),
                [NODE_ADDR],
            ),
            commands,
            readiness,
            failure: failure.clone(),
            listening,
            chain: None,
            records: records.clone(),
            closed: PeerCloses::default(),
            taken: 0,
        };
        sim.scheduler().spawn_root("peer", task.run());
    }
    sim.add_source(Box::new(control));
}
