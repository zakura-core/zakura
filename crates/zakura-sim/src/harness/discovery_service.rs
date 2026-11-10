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
    ZAKURA_CAP_DISCOVERY, ZAKURA_DISCOVERY_STREAM_VERSION, ZAKURA_STREAM_DISCOVERY,
};
use zakura_quic::sim::{SeededInputs, SimNet};

use super::{
    header_sync::wire_config,
    wire_node::{DialReporter, TransportTap, WireEndpoint},
    NetScenario, Subsystem,
};
use crate::{
    domain::discovery::late_response_admitted,
    model::Label,
    observe::StreamId,
    peer::{
        discovery::{
            discovery_stream, Answer, DiscoveryAct, DiscoveryRow, DiscoveryScript, Hold, Request,
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
    let mut control =
        PeerControl::new(failure.clone(), records.clone(), shared_ids).with_discovery(&stream);
    for ((index, peer), name) in peers.iter().enumerate().zip(names) {
        assert!(
            peer.honest.is_none(),
            "a discovery scenario's peers hold no header chain"
        );
        let (readiness, commands) = control.add(
            &name,
            peer.script.clone(),
            peer.adversary.is_some() || peer.discovers_adversarially(),
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

    /// The fixture's name, `discovery-wire-<name>.bin`.
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
    const GENERATOR: u32 = 1;
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

    /// For a scenario where a peer takes a discovery act, `BoundedAdversary`
    /// at [`ADVERSARY_RATE`] actions per virtual second, the budget
    /// [`PeerControl`] holds an adversary to.
    fn assumptions(scenario: &WireScenario) -> Assumptions<Self> {
        if !scenario
            .peers
            .iter()
            .any(|peer| peer.adversary.is_some() || peer.discovers_adversarially())
        {
            return Assumptions::none();
        }
        Assumptions::declare(vec![AssumeRecord {
            assume: Assume::BoundedAdversary,
            params: serde_json::json!({ "actions_per_second": ADVERSARY_RATE }),
        }])
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
