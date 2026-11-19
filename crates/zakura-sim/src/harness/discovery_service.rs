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
//! [`SavedPlan`]s, as `header-sync/wire`'s does. It registers discovery's
//! generated receiver and sender properties ([`wire_properties`],
//! disc-3.1), each `Active` once its planted case or scripted bypass
//! lands, and `discovery/late-response-admitted` (disc-3.4). A peer's
//! [`PeerAction::Discover`](crate::peer::PeerAction::Discover) acts reach
//! the receiver ones ([`act_scenario`]), and the node's own exchange
//! reaches the sender ones.

use std::sync::{Arc, Mutex, PoisonError};

use serde::{Deserialize, Serialize};
use zakura_chain::parameters::Network;
use zakura_network::zakura::{
    testkit::discovery::{
        build_discovery_handle, insert_static_bootstrap_candidates, spawn_native_bootstrap_dialer,
        spawn_native_discovery_dialer,
    },
    wire_codec::WireMessage,
    DiscoveryMessage, DiscoveryService, Service, Stream, TransportObserver, ZakuraDiscoveryConfig,
    ZakuraDiscoveryHandle, ZakuraDiscoveryLocalConfig, ZakuraHandshakeConfig, ZakuraServiceId,
    DEFAULT_DISCOVERY_CONNECTION_HEADROOM, DISCOVERY_INITIAL_EXCHANGE_TIMEOUT,
    MAX_DISCOVERY_RECORDS_PER_RESPONSE, ZAKURA_CAP_DISCOVERY, ZAKURA_DISCOVERY_STREAM_VERSION,
    ZAKURA_STREAM_DISCOVERY,
};
use zakura_quic::sim::{SeededInputs, SimNet};

use super::{
    header_sync::wire_config,
    wire_node::{DialReporter, TransportTap, WireEndpoint},
    NetScenario, ScenarioFamily, Subsystem,
};
use crate::{
    domain::discovery::late_response_admitted,
    model::Label,
    observe::StreamId,
    peer::{
        discovery::{
            discovery_stream, Answer, DiscoveryAct, DiscoveryRow, DiscoveryRule, DiscoveryScript,
            Hold, Request,
        },
        key_index,
        limit::ADVERSARY_RATE,
        peer_addr, PeerAction, PeerCloses, PeerControl, PeerFailure, PeerRecords, PeerSpec,
        PeerTask, NODE_ADDR,
    },
    property::{
        Assume, AssumeRecord, Assumptions, HasView, Property, Source, Sources, Status,
        TriggerHistory,
    },
    table::{sender_owed_properties, sender_properties, table_properties, GeneratedId, Template},
    Observation, ObservationLog, RunConfig, Sim,
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
    /// The peer limit of its `GetPeers`, in place of production's 32. Left
    /// out of a serialized scenario when unset, so saved ones keep their
    /// form.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_sample_limit: Option<usize>,
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
/// on `network`, and `spec`'s verification slots, static candidates and
/// peer limit. It
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
    let peer_sample_limit = spec.peer_sample_limit;
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
                let handle = match peer_sample_limit {
                    None => build_discovery_handle(
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
                    ),
                    // Production's builder, with the peer limit changed.
                    Some(peer_sample_limit) => ZakuraDiscoveryHandle::new(
                        ZakuraDiscoveryLocalConfig {
                            secret_key: SeededInputs::node_key(seed, 0),
                            direct_addrs: vec![NODE_ADDR],
                            services: vec![ZakuraServiceId::discovery()],
                            zakura_protocol_min: handshake.zakura_protocol_min,
                            zakura_protocol_max: handshake.zakura_protocol_max,
                            network_id: handshake.network_id,
                            chain_id: handshake.chain_id,
                            last_authored_sequence: None,
                        },
                        ZakuraDiscoveryConfig {
                            max_zakura_connections: limits.max_connections,
                            discovery_connection_headroom: DEFAULT_DISCOVERY_CONNECTION_HEADROOM
                                .max(bootstrap_count),
                            peer_sample_limit,
                            ..ZakuraDiscoveryConfig::default()
                        },
                        supervisor.subscribe(),
                    ),
                }
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
    let shared_ids = tap.shared_ids();
    sim.add_source(Box::new(tap));
    spawn_node(sim, spec, &network, net.sim(), seed, observer, dials, node);
    // Read before any task runs, since building it draws from the run.
    let stream = discovery_stream();
    let failure = PeerFailure::default();
    let records = PeerRecords::default();
    let mut control = PeerControl::new(sim, "node", failure.clone(), records.clone(), shared_ids)
        .with_discovery(&stream);
    for ((index, peer), name) in peers.iter().enumerate().zip(names) {
        assert!(
            peer.honest.is_none(),
            "a discovery scenario's peers hold no header chain"
        );
        let (readiness, commands) = control.add(
            &name,
            peer.script.clone(),
            is_adversary(peer) && !peer.spams,
        );
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
            order: net.order(),
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
                peer_sample_limit: None,
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

/// A saved Plan's scenario. Each runs as the seed
/// `discovery-wire-<name>`.
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
    /// A peer dials the node and holds its `Peers` until the node's initial
    /// exchange wait times out. The Plan delivers the `Peers` after the
    /// node's wait timed out and before its session ends, so the node reads
    /// a late answer (`discovery/late-response-admitted`).
    LateResponse,
}

/// The records [`SavedPlan::Verification`]'s peer relays.
pub const RELAYED: u8 = 3;

impl SavedPlan {
    /// Every saved Plan.
    pub const ALL: [SavedPlan; 5] = [
        SavedPlan::Exchange,
        SavedPlan::TimedOut,
        SavedPlan::Verification,
        SavedPlan::StaticDial,
        SavedPlan::LateResponse,
    ];

    /// Its name, as in its seed's, `discovery-wire-<name>`.
    pub fn name(self) -> &'static str {
        match self {
            SavedPlan::Exchange => "exchange",
            SavedPlan::TimedOut => "timed-out",
            SavedPlan::Verification => "verification",
            SavedPlan::StaticDial => "static-dial",
            SavedPlan::LateResponse => "late-response",
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
            // The node admits the session at the run's start, so the
            // `Peers` goes as its initial wait times out.
            SavedPlan::LateResponse => dialed(DiscoveryScript {
                relays: 1,
                holds: vec![Hold {
                    answer: Answer::Peers,
                    // Two seconds fit a u64 of milliseconds.
                    until_ms: DISCOVERY_INITIAL_EXCHANGE_TIMEOUT.as_millis() as u64,
                }],
            }),
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

/// When an act scenario's held answers go, in milliseconds past the run's
/// start: before the node's 2 s initial exchange wait ends, so the
/// exchange stays open for the act.
pub const ACT_HOLD_MS: u64 = 1_500;

/// The scenario where `act` reaches its label: the default node, dialed by
/// one peer whose script is `act` alone, on links of 1 to 5 ms, so the
/// act and the node's answers fit the node's initial exchange wait, and
/// each round trip moves the clock. The
/// peer relays one record. It holds its `Services`, so the node's exchange
/// stays open, and also the answer `act` takes. An unsolicited `Services`
/// needs the node's `GetServices` answered, so that act's peer holds its
/// `Peers` instead. An over-owed `Peers` needs a node limit under 32, so
/// that act's node asks for one record.
pub fn act_scenario(act: DiscoveryAct) -> WireScenario {
    let hold = |answer| Hold {
        answer,
        until_ms: ACT_HOLD_MS,
    };
    let mut held = vec![Answer::Services];
    if act
        == (DiscoveryAct::Unsolicited {
            row: DiscoveryRow::Services,
        })
    {
        held = vec![Answer::Peers];
    } else if let Some(answer) = act.takes().filter(|answer| *answer != Answer::Services) {
        held.push(answer);
    }
    let script = DiscoveryScript {
        relays: 1,
        holds: held.into_iter().map(hold).collect(),
    };
    let base = WireScenario::default();
    let mut net = base.net.clone();
    net.default_link.min_delay_ms = 1;
    net.default_link.max_delay_ms = 5;
    WireScenario {
        net,
        node: NodeSpec {
            peer_sample_limit: (act
                == DiscoveryAct::OverOwed {
                    row: DiscoveryRow::Peers,
                })
            .then_some(1),
            ..base.node
        },
        peers: vec![PeerSpec {
            script: vec![PeerAction::Discover { act }],
            ..discovering(PeerSpec::dialing(), script)
        }],
    }
}

/// The second tag byte of the first [`HOSTILE_ACTS`] family. The saved
/// Plans take `1` to `0x1f`.
pub const HOSTILE_TAG_BASE: u8 = 0x20;

/// One act per `<label>-closes` ID a discovery scenario reaches, and the
/// two cadence runs, each with its family's name (mutation plan, mut-6).
/// [`DiscoveryWire`]'s `draw` runs [`act_scenario`] of the act at index
/// `i` for the tag `[TAG, HOSTILE_TAG_BASE + i]`, so the generator reaches
/// stream 4's refusal IDs. Inserting an act moves [`ASKED`]'s tags, so a
/// new act goes in with a bump of [`DiscoveryWire`]'s `GENERATOR`.
pub const HOSTILE_ACTS: [(&str, DiscoveryAct); 29] = [
    (
        "undeclared-type",
        DiscoveryAct::UndeclaredType { message_type: 0 },
    ),
    (
        "hello-reserved-flags",
        DiscoveryAct::ReservedFlags {
            row: DiscoveryRow::Hello,
            bit: 0,
        },
    ),
    (
        "hello-short-payload",
        DiscoveryAct::ShortPayload {
            row: DiscoveryRow::Hello,
        },
    ),
    (
        "hello-long-payload",
        DiscoveryAct::LongPayload {
            row: DiscoveryRow::Hello,
        },
    ),
    (
        "hello-bad-content",
        DiscoveryAct::BadContent {
            rule: DiscoveryRule::HelloSignature,
        },
    ),
    ("hello-burst", DiscoveryAct::HelloBurst),
    (
        "get-peers-reserved-flags",
        DiscoveryAct::ReservedFlags {
            row: DiscoveryRow::GetPeers,
            bit: 0,
        },
    ),
    (
        "get-peers-short-payload",
        DiscoveryAct::ShortPayload {
            row: DiscoveryRow::GetPeers,
        },
    ),
    (
        "get-peers-long-payload",
        DiscoveryAct::LongPayload {
            row: DiscoveryRow::GetPeers,
        },
    ),
    (
        "get-peers-over-margin",
        DiscoveryAct::OverMargin {
            row: DiscoveryRow::GetPeers,
        },
    ),
    (
        "get-peers-bad-content",
        DiscoveryAct::BadContent {
            rule: DiscoveryRule::GetPeersLimit,
        },
    ),
    (
        "get-peers-request-run",
        DiscoveryAct::RequestRun {
            row: DiscoveryRow::GetPeers,
        },
    ),
    (
        "peers-reserved-flags",
        DiscoveryAct::ReservedFlags {
            row: DiscoveryRow::Peers,
            bit: 0,
        },
    ),
    (
        "peers-short-payload",
        DiscoveryAct::ShortPayload {
            row: DiscoveryRow::Peers,
        },
    ),
    (
        "peers-long-payload",
        DiscoveryAct::LongPayload {
            row: DiscoveryRow::Peers,
        },
    ),
    (
        "peers-unsolicited",
        DiscoveryAct::Unsolicited {
            row: DiscoveryRow::Peers,
        },
    ),
    (
        "peers-over-owed",
        DiscoveryAct::OverOwed {
            row: DiscoveryRow::Peers,
        },
    ),
    (
        "peers-bad-content",
        DiscoveryAct::BadContent {
            rule: DiscoveryRule::PeersSignature,
        },
    ),
    (
        "get-services-reserved-flags",
        DiscoveryAct::ReservedFlags {
            row: DiscoveryRow::GetServices,
            bit: 0,
        },
    ),
    (
        "get-services-short-payload",
        DiscoveryAct::ShortPayload {
            row: DiscoveryRow::GetServices,
        },
    ),
    (
        "get-services-long-payload",
        DiscoveryAct::LongPayload {
            row: DiscoveryRow::GetServices,
        },
    ),
    (
        "get-services-over-margin",
        DiscoveryAct::OverMargin {
            row: DiscoveryRow::GetServices,
        },
    ),
    (
        "get-services-bad-content",
        DiscoveryAct::BadContent {
            rule: DiscoveryRule::GetServicesWanted,
        },
    ),
    (
        "get-services-request-run",
        DiscoveryAct::RequestRun {
            row: DiscoveryRow::GetServices,
        },
    ),
    (
        "services-reserved-flags",
        DiscoveryAct::ReservedFlags {
            row: DiscoveryRow::Services,
            bit: 0,
        },
    ),
    (
        "services-short-payload",
        DiscoveryAct::ShortPayload {
            row: DiscoveryRow::Services,
        },
    ),
    (
        "services-long-payload",
        DiscoveryAct::LongPayload {
            row: DiscoveryRow::Services,
        },
    ),
    (
        "services-unsolicited",
        DiscoveryAct::Unsolicited {
            row: DiscoveryRow::Services,
        },
    ),
    (
        "services-bad-content",
        DiscoveryAct::BadContent {
            rule: DiscoveryRule::ServicesNodeId,
        },
    ),
];

/// The requests that ask for less than everything, whose answers reach
/// the `sender-within-owed` IDs in [`asking_scenario`]. Their tags follow
/// [`HOSTILE_ACTS`]'.
pub const ASKED: [Request; 2] = [Request::GetOnePeer, Request::GetHeaderSyncService];

/// The conformant frames that local policy, not the specification, refuses,
/// each with its family's name. A node that closes on one breaks
/// `conformant-never-closes`. Their tags follow [`ASKED`]'.
pub const CONFORMANT_ACTS: [(&str, DiscoveryAct); 2] = [
    ("expired-hello", DiscoveryAct::ExpiredHello),
    ("expired-relay", DiscoveryAct::ExpiredRelay),
];

/// What a drawn family runs: a peer's discovery act ([`act_scenario`]), or
/// an asking request ([`asking_scenario`]).
#[derive(Clone, Copy)]
enum Drawn {
    Act(DiscoveryAct),
    Asked(Request),
}

impl Drawn {
    /// The family's scenario.
    fn scenario(self) -> WireScenario {
        match self {
            Drawn::Act(act) => act_scenario(act),
            Drawn::Asked(request) => asking_scenario(request),
        }
    }
}

/// The families after the saved Plans: each of [`HOSTILE_ACTS`], then
/// each of [`ASKED`], then each of [`CONFORMANT_ACTS`], by name, tag and
/// what it runs. Only [`Drawn::scenario`] builds a scenario, so a draw
/// builds just the family it picks.
fn drawn_families() -> impl Iterator<Item = (&'static str, [u8; 2], Drawn)> {
    let acts = HOSTILE_ACTS
        .iter()
        .map(|(name, act)| (*name, Drawn::Act(*act)));
    let asked = ASKED
        .iter()
        .map(|request| (request.name(), Drawn::Asked(*request)));
    let conformant = CONFORMANT_ACTS
        .iter()
        .map(|(name, act)| (*name, Drawn::Act(*act)));
    acts.chain(asked)
        .chain(conformant)
        .zip(HOSTILE_TAG_BASE..)
        .map(|((name, drawn), tag)| (name, [TAG, tag], drawn))
}

/// The scenario `prefix` tags, if it starts with one of
/// [`drawn_families`]' tags.
fn drawn_scenario(prefix: &[u8]) -> Option<WireScenario> {
    drawn_families()
        .find(|(_, tag, _)| prefix.get(..2) == Some(&tag[..]))
        .map(|(_, _, drawn)| drawn.scenario())
}

/// The honest exchange, with the peer sending `request` once.
pub fn asking_scenario(request: Request) -> WireScenario {
    let mut scenario = WireScenario::default();
    scenario.peers[0].script.push(PeerAction::Ask { request });
    scenario
}

/// The family discovery's generated property IDs name.
pub const FAMILY: &str = "discovery";

/// Discovery's one stream as its generated properties name it: its kind,
/// version and table. A test pins them to [`DiscoveryService::streams`],
/// which needs a handle, and building one draws from the run.
pub const STREAMS: [Stream; 1] = [Stream {
    kind: ZAKURA_STREAM_DISCOVERY,
    version: ZAKURA_DISCOVERY_STREAM_VERSION,
    messages: Some(<DiscoveryMessage as WireMessage>::RULES),
    ..Stream::PERSISTENT
}];

/// `discovery/wire`'s properties: discovery's generated receiver
/// properties, 31 IDs, then its sender properties, 7 IDs, then the
/// `sender-within-owed` IDs of `peers` and `services`
/// ([`sender_owed_properties`]). An ID is a
/// `Draft` until its planted case lands. An ID with a planted case checks
/// that case's trigger history ([`planted_trigger`]). An ID no planted case
/// violates alone, one of [`SCRIPTED`], is `Active` with the scripted
/// bypass in `tests/discovery_wire_witness.rs`, as header sync's are.
/// Then comes `discovery/late-response-admitted` (disc-3.4), a
/// hand-written `Active` property with its own planted case and trigger
/// history.
///
/// A `bad-content-closes` ID's generated check accepts a fault close or a
/// `Misbehavior`, and discovery's node answers bad content with a fault
/// close. Neither comes from storage, which the discovery node doesn't
/// have, so its IDs need only the frames and closes.
///
/// Not registered: the downstream templates. Discovery's harness observes
/// none of the node's serving, work or output, so they would claim coverage
/// that doesn't exist. They follow disc-1's full-service observations.
pub fn wire_properties<S: HasView>() -> Vec<Property<S>> {
    let receiver = table_properties(FAMILY, &STREAMS)
        .into_iter()
        .map(|property| Property {
            needs: &[Source::Frames, Source::Closes],
            ..property
        });
    receiver
        .chain(sender_properties(FAMILY, &STREAMS))
        .chain(sender_owed_properties(FAMILY, &STREAMS))
        .map(|property| match planted_trigger(property.id.as_str()) {
            Some(trigger) => Property {
                witness: Some(TriggerHistory::Log(trigger)),
                ..property
            },
            None if SCRIPTED.contains(&property.id.as_str()) => property,
            None => Property {
                status: Status::Draft,
                ..property
            },
        })
        .chain([late_response_admitted()])
        .collect()
}

/// The generated IDs no scenario reaches, which a scripted bypass violates
/// instead of a planted case (disc-3.2). The node asks for every service,
/// so its `GetServices` owes the row's largest `Services`, and a larger one
/// is `long-payload`.
pub const SCRIPTED: [&str; 1] = ["discovery/s4v2/services/over-owed-closes"];

/// True if the generated ID `id` has a planted case under `qa/planted/`,
/// so it isn't a `Draft`.
pub fn has_planted_case(id: &str) -> bool {
    planted_trigger(id).is_some()
}

/// A trigger history over the run's observation log.
type Trigger = fn(&ObservationLog) -> bool;

/// The trigger history of the generated ID `id`'s planted case, if it has
/// one. Only discovery's own IDs on its stream have one.
///
/// - The stateless `<label>-closes` IDs (disc-3.3): the shared frame reader
///   skips the check for one row of discovery's table, and discovery drops
///   that frame instead of closing.
/// - The IDs whose guard is discovery's own (disc-3.2): a patch bypasses
///   that guard for one row.
///
/// A `<label>-closes` ID's trigger is the node judging a frame with the ID's
/// label on its row, and `conformant-never-closes`'s is the node judging a
/// conformant frame on any row ([`judged_label`]). A `cadence-never-closes`
/// ID's is the node judging a frame on its row that found the row's
/// cadence bucket empty ([`judged_exhausted`]). A sender ID's trigger is the
/// node sending a frame of its row ([`sent_row`]).
fn planted_trigger(id: &str) -> Option<Trigger> {
    let id = GeneratedId::parse(id)?;
    if id.family != FAMILY || id.stream != stream_id() {
        return None;
    }
    let row = || {
        <DiscoveryMessage as WireMessage>::RULES
            .iter()
            .position(|rule| Some(rule.name) == id.row)
    };
    match id.template {
        Template::ConformantNeverCloses => Some(judged_label::<CONFORMANT, ANY_ROW>),
        Template::Closes(label) => {
            match (LABELS.iter().position(|judged| *judged == label)?, row()) {
                (0, _) => Some(judged_label::<0, ANY_ROW>),
                (at @ 1..=3, Some(row)) => Some(ROW_TRIGGERS[at - 1][row]),
                (5, Some(1)) => Some(judged_label::<5, 1>),
                (5, Some(3)) => Some(judged_label::<5, 3>),
                (6, Some(2)) => Some(judged_label::<6, 2>),
                (6, Some(4)) => Some(judged_label::<6, 4>),
                (7, Some(2)) => Some(judged_label::<7, 2>),
                (8, Some(row)) => BAD_CONTENT_TRIGGERS.get(row).copied(),
                _ => None,
            }
        }
        Template::CadenceNeverCloses => CADENCE_TRIGGERS.get(row()?).copied(),
        Template::SenderWithinLimit | Template::SenderEndsOnce | Template::SenderCadence => {
            SENT_TRIGGERS.get(row()?).copied()
        }
        // Every simulated peer binds an RFC 1918 address, which the book's
        // sampler refuses, so the node's `Peers` answers carry no records,
        // and `peers/sender-within-owed` stays a `Draft`.
        Template::SenderWithinOwed if id.row == Some(DiscoveryRow::Services.name()) => {
            SENT_TRIGGERS.get(row()?).copied()
        }
        _ => None,
    }
}

/// The kind and version of discovery's one stream ([`STREAMS`]).
fn stream_id() -> StreamId {
    let [stream] = STREAMS;
    StreamId {
        kind: stream.kind,
        version: stream.version,
    }
}

/// The labels of the judged frames that trigger a planted case, as
/// [`judged_label`] numbers them: first the four the shared frame reader
/// judges from a header alone, then the conformant label and the four
/// whose guard is discovery's own.
const LABELS: [Label; 9] = [
    Label::UndeclaredType,
    Label::ReservedFlags,
    Label::ShortPayload,
    Label::LongPayload,
    Label::Conformant,
    Label::OverMargin,
    Label::Unsolicited,
    Label::OverOwed,
    Label::BadContent,
];

/// `Label::Conformant`'s index in [`LABELS`].
const CONFORMANT: usize = 4;

/// [`judged_label`]'s `ROW` for a frame on any row.
const ANY_ROW: usize = usize::MAX;

/// [`judged_label`] of each row-level stateless label, `LABELS[1..=3]`, by
/// label, then by row in table order.
const ROW_TRIGGERS: [[Trigger; 5]; 3] = [
    [
        judged_label::<1, 0>,
        judged_label::<1, 1>,
        judged_label::<1, 2>,
        judged_label::<1, 3>,
        judged_label::<1, 4>,
    ],
    [
        judged_label::<2, 0>,
        judged_label::<2, 1>,
        judged_label::<2, 2>,
        judged_label::<2, 3>,
        judged_label::<2, 4>,
    ],
    [
        judged_label::<3, 0>,
        judged_label::<3, 1>,
        judged_label::<3, 2>,
        judged_label::<3, 3>,
        judged_label::<3, 4>,
    ],
];

/// [`judged_label`] of `Label::BadContent` by row in table order.
const BAD_CONTENT_TRIGGERS: [Trigger; 5] = [
    judged_label::<8, 0>,
    judged_label::<8, 1>,
    judged_label::<8, 2>,
    judged_label::<8, 3>,
    judged_label::<8, 4>,
];

/// [`judged_exhausted`] by row in table order.
const CADENCE_TRIGGERS: [Trigger; 5] = [
    judged_exhausted::<0>,
    judged_exhausted::<1>,
    judged_exhausted::<2>,
    judged_exhausted::<3>,
    judged_exhausted::<4>,
];

/// [`sent_row`] by row in table order.
const SENT_TRIGGERS: [Trigger; 5] = [
    sent_row::<0>,
    sent_row::<1>,
    sent_row::<2>,
    sent_row::<3>,
    sent_row::<4>,
];

/// True if `log` holds a frame the node judged on discovery's stream that
/// the peer labelled `LABELS[LABEL]`, on the row at `ROW` in discovery's
/// table, or on any row for [`ANY_ROW`]. A trigger history is a plain
/// function, so each ID takes its own instance.
fn judged_label<const LABEL: usize, const ROW: usize>(log: &ObservationLog) -> bool {
    let label = LABELS[LABEL];
    let row = <DiscoveryMessage as WireMessage>::RULES
        .get(ROW)
        .map(|rule| rule.name);
    let stream = stream_id();
    let entries = log.entries();
    entries.iter().enumerate().any(|(at, entry)| {
        let Observation::FrameJudged {
            connection,
            stream: judged,
            quic_stream,
            seq,
        } = entry.obs
        else {
            return false;
        };
        judged == stream
            && entries[..at].iter().any(|sent| {
                matches!(&sent.obs, Observation::PeerFrame {
                    connection: c,
                    stream: s,
                    quic_stream: q,
                    seq: n,
                    row: r,
                    label: l,
                    ..
                } if (*c, *s, *q, *n) == (connection, judged, quic_stream, seq)
                    && *l == label
                    && (row.is_none() || r.as_deref() == row))
            })
    })
}

/// True if `log` holds a frame the node judged on discovery's stream, on
/// the row at `ROW` in discovery's table, that the peer sent into an empty
/// cadence bucket.
fn judged_exhausted<const ROW: usize>(log: &ObservationLog) -> bool {
    let row = <DiscoveryMessage as WireMessage>::RULES[ROW].name;
    let stream = stream_id();
    let entries = log.entries();
    entries.iter().enumerate().any(|(at, entry)| {
        let Observation::FrameJudged {
            connection,
            stream: judged,
            quic_stream,
            seq,
        } = entry.obs
        else {
            return false;
        };
        judged == stream
            && entries[..at].iter().any(|sent| {
                matches!(&sent.obs, Observation::PeerFrame {
                    connection: c,
                    stream: s,
                    quic_stream: q,
                    seq: n,
                    row: Some(r),
                    exhausted: true,
                    ..
                } if (*c, *s, *q, *n) == (connection, judged, quic_stream, seq) && r == row)
            })
    })
}

/// True if `log` holds a frame the node sent on discovery's stream on the
/// row at `ROW` in discovery's table.
fn sent_row<const ROW: usize>(log: &ObservationLog) -> bool {
    let row = <DiscoveryMessage as WireMessage>::RULES[ROW].name;
    let stream = stream_id();
    log.entries().iter().any(|entry| {
        matches!(&entry.obs, Observation::NodeFrame { stream: s, row: Some(r), .. }
            if *s == stream && r == row)
    })
}

/// Whether `peer` is an adversary: it acts, here or on header sync.
fn is_adversary(peer: &PeerSpec) -> bool {
    peer.adversary.is_some() || peer.discovers_adversarially()
}

/// For a scenario where a peer takes a discovery act and none spams,
/// `BoundedAdversary` at [`ADVERSARY_RATE`] actions per virtual second, the
/// budget [`PeerControl`] holds an adversary to. Nothing otherwise, so a
/// spammer's run judges no property that assumes the bound.
fn wire_assumptions<S>(scenario: &WireScenario) -> Assumptions<S> {
    let adversaries: Vec<&PeerSpec> = scenario.peers.iter().filter(|p| is_adversary(p)).collect();
    if adversaries.is_empty() || adversaries.iter().any(|peer| peer.spams) {
        return Assumptions::none();
    }
    Assumptions::declare(vec![AssumeRecord {
        assume: Assume::BoundedAdversary,
        params: serde_json::json!({ "actions_per_second": ADVERSARY_RATE }),
    }])
}

/// The step loop's task observations, the network's datagrams, the peers'
/// frames and the node's closes.
fn wire_sources() -> Sources {
    Sources::observed(&[
        Source::Schedule,
        Source::Datagrams,
        Source::Frames,
        Source::Closes,
    ])
}

/// The action slots of a `discovery/composed` prefix, after its version
/// byte and its three bytes of node, script and hold choices.
pub const COMPOSED_SLOTS: usize = 13;

/// The first byte of every `discovery/composed` prefix that
/// [`composed_scenario`] reads: [`DiscoveryComposed`]'s generator. Any
/// other first byte draws the default scenario, as `header-sync/storage`'s
/// grammar does.
pub const COMPOSED_VERSION: u8 = 1;

/// The actions that keep a composed script's peer conformant: each request,
/// then the cadence runs of [`HOSTILE_ACTS`], whose exhausted frame the
/// node must not close for, then [`CONFORMANT_ACTS`]. A new action goes at
/// the end of its list, with a bump of [`DiscoveryComposed`]'s `GENERATOR`.
fn conformant_actions() -> Vec<PeerAction> {
    let requests = [
        Request::GetPeers,
        Request::GetServices,
        Request::GetOnePeer,
        Request::GetHeaderSyncService,
    ]
    .map(|request| PeerAction::Ask { request });
    let runs = HOSTILE_ACTS
        .iter()
        .filter(|(_, act)| is_cadence_run(*act))
        .chain(&CONFORMANT_ACTS)
        .map(|(_, act)| PeerAction::Discover { act: *act });
    requests.into_iter().chain(runs).collect()
}

/// Every action a composed script may take: [`conformant_actions`], then
/// each act of [`HOSTILE_ACTS`] that breaks a rule.
fn composed_actions() -> Vec<PeerAction> {
    let faults = HOSTILE_ACTS
        .iter()
        .filter(|(_, act)| !is_cadence_run(*act))
        .map(|(_, act)| PeerAction::Discover { act: *act });
    conformant_actions().into_iter().chain(faults).collect()
}

/// Whether `act` is a cadence run, whose frames stay conformant.
fn is_cadence_run(act: DiscoveryAct) -> bool {
    matches!(
        act,
        DiscoveryAct::HelloBurst | DiscoveryAct::RequestRun { .. }
    )
}

/// The scenario the bytes after a `discovery/composed` prefix's version
/// byte encode, in fixed slots so a change to one byte changes one choice
/// (fuzz-11). Counting from the byte after the version:
///
/// - Byte 0: the node's verification slots (bits 0 and 1, 1 to 4), its
///   `GetPeers` limit (bits 2 and 3: production's, 1, 2 or 8), whether it
///   dials a listening peer it holds as a static candidate (bit 4), and the
///   links' delays (bits 5 and 6: 1 to 100 ms, 1 to 5 ms, 50 to 400 ms, or
///   1 to 1,000 ms).
/// - Byte 1: the records each `Peers` relays (bits 0 and 1, 0 to 3, at
///   most the node's limit), and whether the script may break rules
///   (bit 7).
/// - Byte 2: which answers the peer holds (bits 0 to 2: `Hello`, `Peers`,
///   `Services`) and until when (bits 3 and 4: 500, 1,500, 2,500 or 4,000
///   ms past the run's start).
/// - Each later byte: an action slot. 0 ends the script. Any other value
///   picks an action of [`conformant_actions`], or of [`composed_actions`]
///   once byte 1 lets the script break rules.
///
/// The peer skips an act that isn't ready within [`STALE_ACT_WAIT`] of
/// becoming its next action ([`PeerSpec::skip_stale_acts`]), so one act
/// can't stall the rest.
///
/// [`STALE_ACT_WAIT`]: crate::peer::STALE_ACT_WAIT
pub fn composed_scenario(prefix: &[u8]) -> WireScenario {
    let byte = |index: usize| prefix.get(index).copied().unwrap_or(0);
    let (node, script, holds) = (byte(0), byte(1), byte(2));
    let base = WireScenario::default();
    let mut net = base.net.clone();
    // At least a millisecond, so each round trip moves the clock, as
    // `act_scenario`'s links do: a request run's exchanges would otherwise
    // all land at one instant.
    (net.default_link.min_delay_ms, net.default_link.max_delay_ms) = match (node >> 5) & 3 {
        0 => (1, 100),
        1 => (1, 5),
        2 => (50, 400),
        _ => (1, 1_000),
    };
    let peer_sample_limit = [None, Some(1), Some(2), Some(8)][usize::from((node >> 2) & 3)];
    let limit = peer_sample_limit.unwrap_or(MAX_DISCOVERY_RECORDS_PER_RESPONSE);
    let until_ms = [500, 1_500, 2_500, 4_000][usize::from((holds >> 3) & 3)];
    let held = [Answer::Hello, Answer::Peers, Answer::Services]
        .into_iter()
        .zip(0..)
        .filter(|(_, bit)| holds & (1 << bit) != 0)
        .map(|(answer, _)| Hold { answer, until_ms })
        .collect();
    let actions = if script & 0x80 != 0 {
        composed_actions()
    } else {
        conformant_actions()
    };
    let steps = (3..3 + COMPOSED_SLOTS)
        .map(byte)
        .take_while(|slot| *slot != 0)
        // The remainder is below the actions' count.
        .map(|slot| actions[usize::from(slot - 1) % actions.len()])
        .collect();
    let dials = node & 0x10 != 0;
    let peer = if dials {
        PeerSpec::listening()
    } else {
        PeerSpec::dialing()
    };
    let relays =
        u8::try_from(usize::from(script & 3).min(limit)).expect("at most three relayed records");
    WireScenario {
        net,
        node: NodeSpec {
            verification_slots: 1 + usize::from(node & 3),
            static_candidates: if dials { vec![0] } else { Vec::new() },
            peer_sample_limit,
        },
        peers: vec![PeerSpec {
            script: steps,
            skip_stale_acts: true,
            ..discovering(
                peer,
                DiscoveryScript {
                    relays,
                    holds: held,
                },
            )
        }],
    }
}

/// The full `discovery/composed` prefix, [`COMPOSED_VERSION`] first, whose
/// scenario runs `steps` under bytes `node`, `script` and `holds`
/// ([`composed_scenario`]), with each step's slot its first value. Fails if
/// `steps` holds more than [`COMPOSED_SLOTS`] actions, or an action that
/// `script` doesn't allow.
pub fn composed_prefix(
    node: u8,
    script: u8,
    holds: u8,
    steps: &[PeerAction],
) -> Result<[u8; COMPOSED_BYTES], String> {
    if steps.len() > COMPOSED_SLOTS {
        return Err(format!("{} steps, past {COMPOSED_SLOTS}", steps.len()));
    }
    let actions = if script & 0x80 != 0 {
        composed_actions()
    } else {
        conformant_actions()
    };
    let mut prefix = [0; COMPOSED_BYTES];
    prefix[..4].copy_from_slice(&[COMPOSED_VERSION, node, script, holds]);
    for (slot, step) in prefix[4..].iter_mut().zip(steps) {
        let index = actions
            .iter()
            .position(|action| action == step)
            .ok_or_else(|| format!("byte 1 {script:#04x} allows no {}", step.name()))?;
        *slot = u8::try_from(index + 1)
            .map_err(|_| format!("{} actions, past a byte's slots", actions.len()))?;
    }
    Ok(prefix)
}

/// `discovery/wire`'s node and properties with one peer whose script,
/// holds, links and node limits a prefix composes ([`composed_scenario`]).
/// The fuzzer then chooses the order, count and mix of the peer's
/// exchanges, where `discovery/wire` picks one saved conversation.
pub struct DiscoveryComposed;

impl HasView for DiscoveryComposed {
    type View = ();

    /// Nothing. The registry's checks read the observations.
    fn view(&self) {}
}

/// The length of a `discovery/composed` prefix: its version byte, three
/// bytes of choices and [`COMPOSED_SLOTS`] action slots.
pub const COMPOSED_BYTES: usize = 4 + COMPOSED_SLOTS;

/// `discovery/composed`'s step budget: room for a script of
/// [`COMPOSED_SLOTS`] acts, past [`wire_config`]'s 30,000 for one act. Its
/// livelock limit, a tenth of the budget, leaves room for the two bursts of
/// up to 64 frames the adversary's rate lets land at one instant, at about
/// 40 steps a frame.
pub const COMPOSED_BUDGET: u64 = 80_000;

impl Subsystem for DiscoveryComposed {
    const NAME: &'static str = "discovery/composed";
    // A u8 always fits in a u32.
    const GENERATOR: u32 = COMPOSED_VERSION as u32;
    const SCENARIO_BYTES: usize = COMPOSED_BYTES;
    type Scenario = WireScenario;
    type Prepared = NodeHandle;

    /// [`wire_config`] with [`COMPOSED_BUDGET`] steps.
    fn config(_: &WireScenario) -> RunConfig {
        RunConfig {
            budget: COMPOSED_BUDGET,
            ..wire_config()
        }
    }

    /// [`wire_properties`].
    fn properties() -> Vec<Property<Self>> {
        wire_properties()
    }

    /// [`wire_assumptions`].
    fn assumptions(scenario: &WireScenario) -> Assumptions<Self> {
        wire_assumptions(scenario)
    }

    /// An empty slot for the node's handle.
    fn prepare(_: &WireScenario) -> NodeHandle {
        NodeHandle::default()
    }

    /// [`spawn`]'s network, node and peers, with the node's handle kept in
    /// `node`.
    fn build(sim: &mut Sim, scenario: &WireScenario, node: NodeHandle) -> Self {
        spawn_with(sim, &scenario.net, &scenario.node, &scenario.peers, node);
        DiscoveryComposed
    }

    /// [`wire_sources`].
    fn sources() -> Sources {
        wire_sources()
    }

    /// One family, the grammar, whose tag is [`COMPOSED_VERSION`].
    fn families() -> Vec<ScenarioFamily> {
        vec![ScenarioFamily::new("composed", [COMPOSED_VERSION])]
    }

    /// Whether `prefix` starts with [`COMPOSED_VERSION`], so `draw` reads
    /// it rather than drawing the default scenario.
    fn accepts(prefix: &[u8]) -> bool {
        prefix.first() == Some(&COMPOSED_VERSION)
    }

    /// [`composed_scenario`] of the bytes after [`COMPOSED_VERSION`], or the
    /// default scenario for a prefix with another first byte.
    fn draw(prefix: &[u8]) -> WireScenario {
        match prefix.split_first() {
            Some((&COMPOSED_VERSION, grammar)) => composed_scenario(grammar),
            _ => WireScenario::default(),
        }
    }
}

/// The first byte of every `discovery/spam` prefix that [`spam_scenario`]
/// reads: [`DiscoverySpam`]'s generator. Any other first byte draws the
/// default scenario.
pub const SPAM_VERSION: u8 = 1;

/// The request slots of a `discovery/spam` prefix's honest peer.
pub const SPAM_HONEST_SLOTS: usize = 4;

/// The action slots a `discovery/spam` prefix's spammers share.
pub const SPAM_SLOTS: usize = 24;

/// The length of a `discovery/spam` prefix: its version byte, two bytes of
/// choices, [`SPAM_HONEST_SLOTS`] and [`SPAM_SLOTS`].
pub const SPAM_BYTES: usize = 3 + SPAM_HONEST_SLOTS + SPAM_SLOTS;

/// `discovery/spam`'s step budget: room for three spammers' scripts of
/// [`SPAM_SLOTS`] acts beside the honest peer. Its runs declare no
/// `BoundedAdversary`, so no livelock limit applies.
pub const SPAM_BUDGET: u64 = 150_000;

/// The requests a `discovery/spam` honest peer may ask. A new one goes at
/// the end, with a bump of [`DiscoverySpam`]'s `GENERATOR`.
const HONEST_REQUESTS: [Request; 4] = [
    Request::GetPeers,
    Request::GetServices,
    Request::GetOnePeer,
    Request::GetHeaderSyncService,
];

/// The answer a `discovery/spam` honest peer holds. The node closes a
/// discovery connection once its own exchange completes, so the held
/// `Services` keeps the honest connection open while the peer asks.
const HONEST_HOLD: Hold = Hold {
    answer: Answer::Services,
    until_ms: 1_500,
};

/// Every action a spammer may take: [`composed_actions`], then a
/// `Disconnect`, after which it dials again.
fn spam_actions() -> Vec<PeerAction> {
    composed_actions()
        .into_iter()
        .chain([PeerAction::Disconnect])
        .collect()
}

/// The scenario the bytes after a `discovery/spam` prefix's version byte
/// encode, in fixed slots (fuzz-11). Counting from the byte after the
/// version:
///
/// - Byte 0: the node's verification slots, `GetPeers` limit and links, as
///   [`composed_scenario`]'s byte 0 sets them. The node dials no one.
/// - Byte 1: the records each spammer's `Peers` relays (bits 0 and 1, at
///   most the node's limit), and how many spammers run (bits 4 and 5: 1, 2,
///   3 or 3).
/// - Bytes 2 to 5: the honest peer's requests, of [`HONEST_REQUESTS`]. 0
///   ends its script.
/// - Each later byte: an action slot of [`spam_actions`], which every
///   spammer runs. 0 ends the script.
///
/// Each spammer [spams](PeerSpec::spams), and skips stale acts as a
/// composed peer does. The honest peer dials last, holds
/// [`HONEST_HOLD`] and only asks, so `conformant-never-closes` judges every
/// frame it sends.
pub fn spam_scenario(prefix: &[u8]) -> WireScenario {
    let byte = |index: usize| prefix.get(index).copied().unwrap_or(0);
    let mut scenario = composed_scenario(&[byte(0) & !0x10, byte(1) & 3, 0]);
    let peer = scenario
        .peers
        .pop()
        .expect("a composed scenario has one peer");
    let actions = spam_actions();
    let script: Vec<PeerAction> = (2 + SPAM_HONEST_SLOTS..2 + SPAM_HONEST_SLOTS + SPAM_SLOTS)
        .map(byte)
        .take_while(|slot| *slot != 0)
        // The remainder is below the actions' count.
        .map(|slot| actions[usize::from(slot - 1) % actions.len()])
        .collect();
    let asks = (2..2 + SPAM_HONEST_SLOTS)
        .map(byte)
        .take_while(|slot| *slot != 0)
        .map(|slot| PeerAction::Ask {
            request: HONEST_REQUESTS[usize::from(slot - 1) % HONEST_REQUESTS.len()],
        })
        .collect();
    let spammers = [1, 2, 3, 3][usize::from((byte(1) >> 4) & 3)];
    let spammer = PeerSpec {
        script,
        spams: true,
        ..peer.clone()
    };
    let discovery = peer.discovery.clone().map(|script| DiscoveryScript {
        holds: vec![HONEST_HOLD],
        ..script
    });
    scenario.peers = vec![spammer; spammers];
    scenario.peers.push(PeerSpec {
        script: asks,
        discovery,
        ..peer
    });
    scenario
}

/// The full `discovery/spam` prefix, [`SPAM_VERSION`] first, whose scenario
/// runs `spammers` spammers on `steps` beside an honest peer that asks
/// `asks`, under node byte `node` ([`spam_scenario`]). Fails if `spammers`
/// isn't 1 to 3, a list is longer than its slots, or a step isn't one of
/// [`spam_actions`].
pub fn spam_prefix(
    node: u8,
    spammers: u8,
    asks: &[Request],
    steps: &[PeerAction],
) -> Result<[u8; SPAM_BYTES], String> {
    if !(1..=3).contains(&spammers) {
        return Err(format!("{spammers} spammers, not 1 to 3"));
    }
    if asks.len() > SPAM_HONEST_SLOTS || steps.len() > SPAM_SLOTS {
        return Err(format!("{} asks and {} steps", asks.len(), steps.len()));
    }
    let mut prefix = [0; SPAM_BYTES];
    prefix[..3].copy_from_slice(&[SPAM_VERSION, node, (spammers - 1) << 4]);
    let slot = |index: Option<usize>, what: &str| {
        let index = index.ok_or_else(|| format!("a spam prefix holds no {what}"))?;
        u8::try_from(index + 1).map_err(|_| format!("{what} is past a byte's slots"))
    };
    for (byte, ask) in prefix[3..].iter_mut().zip(asks) {
        *byte = slot(
            HONEST_REQUESTS.iter().position(|request| request == ask),
            ask.name(),
        )?;
    }
    let actions = spam_actions();
    for (byte, step) in prefix[3 + SPAM_HONEST_SLOTS..].iter_mut().zip(steps) {
        *byte = slot(
            actions.iter().position(|action| action == step),
            step.name(),
        )?;
    }
    Ok(prefix)
}

/// `discovery/wire`'s node and properties with one to three spammers and
/// an honest peer beside them ([`spam_scenario`]). A spammer takes its acts
/// as fast as the schedule allows and dials again after every close, so
/// the node's refusals, cadence and per-connection state face a flood that
/// keeps going after the first refusal. Its runs declare no
/// `BoundedAdversary`, so they judge the bounds, panics and the honest
/// peer's `conformant-never-closes`, not liveness.
pub struct DiscoverySpam;

impl HasView for DiscoverySpam {
    type View = ();

    /// Nothing. The registry's checks read the observations.
    fn view(&self) {}
}

impl Subsystem for DiscoverySpam {
    const NAME: &'static str = "discovery/spam";
    // A u8 always fits in a u32.
    const GENERATOR: u32 = SPAM_VERSION as u32;
    const SCENARIO_BYTES: usize = SPAM_BYTES;
    type Scenario = WireScenario;
    type Prepared = NodeHandle;

    /// [`wire_config`] with [`SPAM_BUDGET`] steps.
    fn config(_: &WireScenario) -> RunConfig {
        RunConfig {
            budget: SPAM_BUDGET,
            ..wire_config()
        }
    }

    /// [`wire_properties`].
    fn properties() -> Vec<Property<Self>> {
        wire_properties()
    }

    /// [`wire_assumptions`], which declares nothing for a spammer's run.
    fn assumptions(scenario: &WireScenario) -> Assumptions<Self> {
        wire_assumptions(scenario)
    }

    /// An empty slot for the node's handle.
    fn prepare(_: &WireScenario) -> NodeHandle {
        NodeHandle::default()
    }

    /// [`spawn`]'s network, node and peers, with the node's handle kept in
    /// `node`.
    fn build(sim: &mut Sim, scenario: &WireScenario, node: NodeHandle) -> Self {
        spawn_with(sim, &scenario.net, &scenario.node, &scenario.peers, node);
        DiscoverySpam
    }

    /// [`wire_sources`].
    fn sources() -> Sources {
        wire_sources()
    }

    /// One family, the grammar, whose tag is [`SPAM_VERSION`].
    fn families() -> Vec<ScenarioFamily> {
        vec![ScenarioFamily::new("spam", [SPAM_VERSION])]
    }

    /// Whether `prefix` starts with [`SPAM_VERSION`], so `draw` reads it
    /// rather than drawing the default scenario.
    fn accepts(prefix: &[u8]) -> bool {
        prefix.first() == Some(&SPAM_VERSION)
    }

    /// [`spam_scenario`] of the bytes after [`SPAM_VERSION`], or the default
    /// scenario for a prefix with another first byte.
    fn draw(prefix: &[u8]) -> WireScenario {
        match prefix.split_first() {
            Some((&SPAM_VERSION, grammar)) => spam_scenario(grammar),
            _ => WireScenario::default(),
        }
    }
}

/// The discovery node behind its real transport on simulated QUIC, named
/// `node`, with scripted peers.
pub struct DiscoveryWire;

impl HasView for DiscoveryWire {
    type View = ();

    /// Nothing. The registry's checks read the observations.
    fn view(&self) {}
}

impl Subsystem for DiscoveryWire {
    const NAME: &'static str = "discovery/wire";
    const GENERATOR: u32 = 2;
    const SCENARIO_BYTES: usize = 2;
    type Scenario = WireScenario;
    type Prepared = NodeHandle;

    /// [`wire_config`].
    fn config(_: &WireScenario) -> RunConfig {
        wire_config()
    }

    /// [`wire_properties`].
    fn properties() -> Vec<Property<Self>> {
        wire_properties()
    }

    /// [`wire_assumptions`].
    fn assumptions(scenario: &WireScenario) -> Assumptions<Self> {
        wire_assumptions(scenario)
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

    /// [`wire_sources`].
    fn sources() -> Sources {
        wire_sources()
    }

    /// Each saved Plan's tag ([`SavedPlan::tag`]), then each hostile
    /// act's and asking request's ([`drawn_families`]).
    fn families() -> Vec<ScenarioFamily> {
        SavedPlan::ALL
            .into_iter()
            .map(|plan| ScenarioFamily::new(plan.name(), plan.tag()))
            .chain(drawn_families().map(|(name, tag, _)| ScenarioFamily::new(name, tag)))
            .collect()
    }

    /// The saved Plan's scenario `prefix` tags ([`SavedPlan::tag`]), the
    /// hostile act's or asking request's ([`drawn_families`]), or the
    /// default scenario.
    fn draw(prefix: &[u8]) -> WireScenario {
        if let Some(plan) = SavedPlan::from_tag(prefix) {
            return plan.scenario();
        }
        drawn_scenario(prefix).unwrap_or_default()
    }
}
