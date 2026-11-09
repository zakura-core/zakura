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
//!
//! A peer with a discovery script runs it on its discovery stream
//! (disc-2.2), with the stream `DiscoveryService::streams()` declares, and
//! the peers' evidence logs its frames there.
//!
//! `discovery/wire` (disc-2.3) is the [`Subsystem`] that runs the node and
//! its peers. A committed input's two-byte prefix picks one of its
//! [`SavedPlan`]s, as `header-sync/wire`'s does. It registers only the
//! registry's own checks, and disc-3 adds discovery's properties.

use std::sync::{Arc, Mutex, PoisonError};

use serde::{Deserialize, Serialize};
use zakura_chain::parameters::Network;
use zakura_network::zakura::{
    testkit::discovery::{
        build_discovery_handle, insert_static_bootstrap_candidates, spawn_native_bootstrap_dialer,
        spawn_native_discovery_dialer,
    },
    DiscoveryService, Service, TransportObserver, ZakuraDiscoveryHandle, ZakuraHandshakeConfig,
    ZakuraServiceId, ZAKURA_CAP_DISCOVERY,
};
use zakura_quic::sim::{SeededInputs, SimNet};

use super::{
    header_sync::wire_config,
    wire_node::{DialReporter, TransportTap, WireEndpoint},
    NetScenario, Subsystem,
};
use crate::{
    peer::{
        discovery::{discovery_stream, Answer, DiscoveryScript, Hold},
        key_index, peer_addr, PeerCloses, PeerControl, PeerFailure, PeerRecords, PeerSpec,
        PeerTask, NODE_ADDR,
    },
    property::{HasView, Source, Sources},
    RunConfig, Sim,
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

/// The discovery node's handle, which the node's root task sets once it
/// builds it. Cloning shares it, so a test can read the node's book after a
/// run.
#[derive(Clone, Default)]
pub struct NodeHandle(Arc<Mutex<Option<ZakuraDiscoveryHandle>>>);

impl NodeHandle {
    /// The node's handle, once the run has built it.
    pub fn get(&self) -> Option<ZakuraDiscoveryHandle> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Keeps a clone of `handle`.
    fn set(&self, handle: &ZakuraDiscoveryHandle) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(handle.clone());
    }
}

/// Binds the discovery node to [`NODE_ADDR`] on `net` as
/// [`WireEndpoint::spawn`] does, with discovery alone at production defaults
/// on `network`, and `spec`'s verification slots and static candidates. It
/// reports to `observer`, with failed dials to `dials`, and keeps a clone of
/// its handle in `node`.
#[allow(clippy::too_many_arguments)]
pub fn spawn_node(
    sim: &mut Sim,
    spec: &NodeSpec,
    network: &Network,
    net: SimNet,
    seed: [u8; 32],
    observer: Arc<dyn TransportObserver>,
    dials: DialReporter,
    node: NodeHandle,
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
                node.set(&handle);
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
/// A peer with a discovery script runs it on discovery's stream.
///
/// # Panics
///
/// If a peer runs the honest script, which needs a header chain.
pub fn spawn(sim: &mut Sim, net: &NetScenario, spec: &NodeSpec, peers: &[PeerSpec]) {
    spawn_with(sim, net, spec, peers, NodeHandle::default());
}

/// [`spawn`], keeping the node's handle in `node`.
fn spawn_with(
    sim: &mut Sim,
    net: &NetScenario,
    spec: &NodeSpec,
    peers: &[PeerSpec],
    node: NodeHandle,
) {
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
    spawn_node(sim, spec, &network, net.sim(), seed, observer, dials, node);
    // Read before any task runs, since building it draws from the run.
    let stream = discovery_stream();
    let failure = PeerFailure::default();
    let records = PeerRecords::default();
    let mut control = PeerControl::new(failure.clone(), records.clone()).with_discovery(&stream);
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
        sim.scheduler()
            .spawn_root("peer", task.run_with(Some(stream)));
    }
    sim.add_source(Box::new(control));
}

/// The `discovery/wire` scenario.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireScenario {
    /// The network: its seed, links and loss gate.
    #[serde(default)]
    pub net: NetScenario,
    /// The node's own config.
    pub node: NodeSpec,
    /// The scripted peers, named `peer-<i>` by index.
    pub peers: Vec<PeerSpec>,
}

impl Default for WireScenario {
    /// The node with one verification slot and no static candidate, dialed
    /// by one peer that offers discovery alone and answers the exchange.
    fn default() -> Self {
        WireScenario {
            net: NetScenario::default(),
            node: NodeSpec {
                verification_slots: 1,
                static_candidates: Vec::new(),
            },
            peers: vec![discovering(PeerSpec::dialing(), DiscoveryScript::default())],
        }
    }
}

/// `spec`, offering discovery alone and running `script` on it.
fn discovering(spec: PeerSpec, script: DiscoveryScript) -> PeerSpec {
    PeerSpec {
        capabilities: ZAKURA_CAP_DISCOVERY,
        discovery: Some(script),
        ..spec
    }
}

/// The first byte of every saved Plan's tag.
const TAG: u8 = 0xFE;

/// A saved Plan's scenario. Each has a committed input under
/// `tests/fixtures/discovery-wire-<name>.bin` and a pinned end record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SavedPlan {
    /// A peer dials the node, and both sides exchange `Hello`, `GetPeers`
    /// and `GetServices`. The peer's `Peers` relays one record.
    Exchange,
    /// A peer dials the node and holds its `Hello` past the node's initial
    /// exchange wait, so the node times the exchange out. It answers
    /// `GetPeers` and `GetServices`.
    TimedOut,
    /// A peer dials the node, which has one verification slot, and relays
    /// [`RELAYED`] records in its `Peers`.
    Verification,
    /// The node dials a listening peer it holds as a static candidate.
    StaticDial,
}

/// The records [`SavedPlan::Verification`]'s peer relays.
pub const RELAYED: u8 = 3;

impl SavedPlan {
    /// Every saved Plan.
    pub const ALL: [SavedPlan; 4] = [
        SavedPlan::Exchange,
        SavedPlan::TimedOut,
        SavedPlan::Verification,
        SavedPlan::StaticDial,
    ];

    /// The fixture's name, `discovery-wire-<name>.bin`.
    pub fn name(self) -> &'static str {
        match self {
            SavedPlan::Exchange => "exchange",
            SavedPlan::TimedOut => "timed-out",
            SavedPlan::Verification => "verification",
            SavedPlan::StaticDial => "static-dial",
        }
    }

    /// The two-byte scenario prefix that picks it: `0xFE`, then its place
    /// in [`SavedPlan::ALL`] from 1.
    pub fn tag(self) -> [u8; 2] {
        let index = SavedPlan::ALL
            .iter()
            .position(|plan| *plan == self)
            .expect("every saved Plan is listed");
        // A handful of Plans fit a byte.
        [TAG, (index + 1) as u8]
    }

    /// The saved Plan `prefix` tags, if any.
    pub fn from_tag(prefix: &[u8]) -> Option<SavedPlan> {
        SavedPlan::ALL
            .into_iter()
            .find(|plan| prefix.get(..2) == Some(&plan.tag()[..]))
    }

    /// Its scenario: the default node, with one peer running its script.
    pub fn scenario(self) -> WireScenario {
        match self {
            SavedPlan::Exchange => dialed(DiscoveryScript {
                relays: 1,
                ..DiscoveryScript::default()
            }),
            // Past the node's 2 s initial exchange wait.
            SavedPlan::TimedOut => dialed(DiscoveryScript {
                holds: vec![Hold {
                    answer: Answer::Hello,
                    until_ms: 3_000,
                }],
                ..DiscoveryScript::default()
            }),
            SavedPlan::Verification => dialed(DiscoveryScript {
                relays: RELAYED,
                ..DiscoveryScript::default()
            }),
            SavedPlan::StaticDial => {
                let base = WireScenario::default();
                WireScenario {
                    node: NodeSpec {
                        static_candidates: vec![0],
                        ..base.node
                    },
                    peers: vec![discovering(
                        PeerSpec::listening(),
                        DiscoveryScript::default(),
                    )],
                    ..base
                }
            }
        }
    }
}

/// The default scenario, with its one dialing peer running `script`.
fn dialed(script: DiscoveryScript) -> WireScenario {
    WireScenario {
        peers: vec![discovering(PeerSpec::dialing(), script)],
        ..WireScenario::default()
    }
}

/// The discovery node behind its real transport on simulated QUIC, named
/// `node`, with scripted peers. It registers only the registry's checks.
pub struct DiscoveryWire;

impl HasView for DiscoveryWire {
    type View = ();

    /// Nothing. The registry's checks read the observations.
    fn view(&self) {}
}

impl Subsystem for DiscoveryWire {
    const NAME: &'static str = "discovery/wire";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 2;
    type Scenario = WireScenario;
    type Prepared = NodeHandle;

    /// [`wire_config`].
    fn config(_: &WireScenario) -> RunConfig {
        wire_config()
    }

    /// An empty slot for the node's handle.
    fn prepare(_: &WireScenario) -> NodeHandle {
        NodeHandle::default()
    }

    /// [`spawn`]'s network, node and peers, with the node's handle kept in
    /// `node`.
    fn build(sim: &mut Sim, scenario: &WireScenario, node: NodeHandle) -> Self {
        spawn_with(sim, &scenario.net, &scenario.node, &scenario.peers, node);
        DiscoveryWire
    }

    /// The step loop's task observations, the network's datagrams, the
    /// peers' frames and the node's closes.
    fn sources() -> Sources {
        Sources::observed(&[
            Source::Schedule,
            Source::Datagrams,
            Source::Frames,
            Source::Closes,
        ])
    }

    /// The saved Plan's scenario `prefix` tags ([`SavedPlan::tag`]), or
    /// the default scenario.
    fn draw(prefix: &[u8]) -> WireScenario {
        SavedPlan::from_tag(prefix).map_or_else(WireScenario::default, SavedPlan::scenario)
    }
}
