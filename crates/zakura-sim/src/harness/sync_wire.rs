//! `sync/wire` (xsync-3): `sync/storage`'s node and mirrors, each behind a
//! real `ZakuraProtocolHandler` on its own `zakura-quic` endpoint, bound to
//! the run's `SimNet`. Each handler registers header sync, block sync and
//! discovery, so every connection carries all three services' streams, as
//! a production node's does.
//!
//! The node's actors, taps and engine watch are `sync/storage`'s
//! ([`spawn_actors`]). Only the node's transport is tapped
//! ([`TransportTap`]). A mirror's transport reports to no one.
//!
//! Each connection's dialer redials it once it ends, [`REDIAL_SETTLE`]
//! later, until the node shuts down. So a `Close` step, which disconnects
//! the node's connection to a mirror, replaces that connection.
//!
//! A run ends on its step budget ([`wire_config`]), not idle: QUIC's and
//! the services' timers keep firing after the node syncs. So each saved
//! scenario's end record ends on `budget`, and a check that waits for no
//! task to run never sees that state here.

use std::{
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, PoisonError,
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use zakura_network::zakura::{
    testkit::discovery::build_discovery_handle, DiscoveryService, HeaderSyncService, Service,
    TransportEvent, TransportObserver, ZakuraHandshakeConfig, ZakuraServiceId,
};
use zakura_quic::{sim::SeededInputs, NodeAddr};
use zakura_rt::sync::watch;

use super::{
    block_node::{
        policy::{self as block_policy, Trigger},
        LateHandle,
    },
    header_sync::Dialer,
    sync::{
        spawn_actors, sync_config, sync_properties, Control, QuicLinks, SavedSync, SyncAction,
        SyncMirror, SyncPorts, SyncScenario, SyncStep, SyncStorage, SyncTopology, SyncView,
        Transport,
    },
    wire_node::{Bound, TransportTap, WireEndpoint},
    NetScenario, Subsystem,
};
use crate::{
    domain::session,
    peer::{key_index, peer_addr, peer_id, NODE_ADDR},
    property::{Capture, HasView, Property, Source, Sources},
    RunConfig, Sim,
};

/// How long a dialer waits after its connection ends before it dials
/// again: the production handshake timeout, so the node has torn the old
/// connection down and doesn't refuse the new one as a duplicate.
pub const REDIAL_SETTLE: Duration = Duration::from_secs(10);

/// The most connections one dialer makes over a run.
const MAX_INCARNATIONS: usize = 4;

/// The `sync/wire` scenario: a `sync/storage` scenario on a simulated
/// network.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncWireScenario {
    /// The network and the seed of every endpoint's keys.
    pub net: NetScenario,
    /// The chain, the mirrors, both storages' policies and the control
    /// steps.
    pub sync: SyncScenario,
}

/// The node under test, with header sync, block sync and discovery behind
/// one handler, and mirror peers behind their own. See the module doc.
pub struct SyncWire {
    view: SyncView,
    /// The node's block storage's replies, cleared once the run ends.
    replies: Arc<LateHandle>,
    /// The actors, which live until the run ends.
    _topology: Rc<SyncTopology>,
    /// Every endpoint, the node's first.
    _endpoints: Vec<WireEndpoint>,
}

impl HasView for SyncWire {
    type View = SyncView;

    /// Drops the node's block-sync handle from its block storage's
    /// replies. The handle reaches the storage again through the reactor's
    /// range source, so the cycle would outlive the run.
    fn ended(&self, _: &crate::RunEnd) {
        let _ = self.replies.clear();
    }

    /// The node's block-sync view and the cross model, shared rather than
    /// copied.
    fn view(&self) -> SyncView {
        self.view.clone()
    }

    /// The step of the block tap's last observation, or `Missing` before
    /// its first.
    fn captured(&self) -> Capture {
        self.view
            .block
            .borrow()
            .step
            .map_or(Capture::Missing, Capture::AfterStep)
    }
}

/// A transport observer that drops every event: a mirror's, since a run
/// taps only the node's transport.
#[derive(Debug)]
struct Unobserved;

impl TransportObserver for Unobserved {
    fn observe(&self, _: TransportEvent) {}
}

/// Live steps a wire run allows per fixture block, past
/// [`sync_config`]'s: each frame crosses QUIC's handshakes, acknowledgements
/// and timers.
const WIRE_STEPS_PER_BLOCK: u64 = 6_000;

/// [`sync_config`], with room for QUIC's own steps for each fixture block
/// and each block a `Control` step grows or forks.
pub fn wire_config(scenario: &SyncWireScenario) -> RunConfig {
    let config = sync_config(&scenario.sync);
    let blocks = u64::from(scenario.sync.blocks) + scenario.sync.changed();
    RunConfig {
        budget: scenario
            .sync
            .steps
            .unwrap_or(40_000 + WIRE_STEPS_PER_BLOCK * blocks),
        ..config
    }
}

/// What one actor's handler registers: its header and block services and
/// a discovery service, keyed by the actor's endpoint key.
struct Services {
    header: Arc<dyn Service>,
    block: Arc<dyn Service>,
    key: u32,
    addr: std::net::SocketAddr,
}

impl Services {
    /// The handler's services, with discovery built on `supervisor` and
    /// `limits` as production builds it, advertising header sync. Returns
    /// the discovery handle too, which must live as long as the node.
    fn register(
        self,
        seed: [u8; 32],
        handshake: &ZakuraHandshakeConfig,
        supervisor: &zakura_network::zakura::ZakuraSupervisorHandle,
        limits: &zakura_network::zakura::ZakuraLocalLimits,
    ) -> (
        Vec<Arc<dyn Service>>,
        zakura_network::zakura::ZakuraDiscoveryHandle,
    ) {
        let discovery = build_discovery_handle(
            SeededInputs::node_key(seed, self.key),
            vec![self.addr],
            vec![ZakuraServiceId::discovery(), ZakuraServiceId::header_sync()],
            handshake,
            limits.max_connections,
            0,
            supervisor.subscribe(),
        )
        .expect("production's config on the fixture network is valid");
        // Header sync's and block sync's sessions keep a connection open
        // past discovery's exchange, as zakurad's registry wires them.
        let service: Arc<dyn Service> = Arc::new(
            DiscoveryService::with_max_connections(discovery.clone(), limits.max_connections)
                .with_connection_owners(vec![self.header.clone(), self.block.clone()]),
        );
        (vec![self.header, self.block, service], discovery)
    }
}

/// Spawns a task that dials `target` from the endpoint `from` serves once
/// the endpoint `to` serves too, serves each connection until it ends, and
/// dials again [`REDIAL_SETTLE`] later, until `stopped` is set or it has
/// made [`MAX_INCARNATIONS`] connections.
fn redial(
    sim: &mut Sim,
    mut from: watch::Receiver<Bound>,
    mut to: watch::Receiver<Bound>,
    target: NodeAddr,
    stopped: Arc<AtomicBool>,
) {
    sim.scheduler().spawn_root("redial", async move {
        let bound = match from.wait_for(Option::is_some).await {
            Ok(bound) => bound.clone(),
            Err(_) => return,
        };
        if to.wait_for(Option::is_some).await.is_err() {
            return;
        }
        let (endpoint, limits) = bound.expect("the wait returns a bound endpoint");
        for _ in 0..MAX_INCARNATIONS {
            if stopped.load(Ordering::SeqCst) {
                return;
            }
            // A failed dial shows in the node's transport facts, or as a
            // missing session.
            let _ = endpoint.dial(target.clone(), &limits).await;
            if stopped.load(Ordering::SeqCst) {
                return;
            }
            zakura_rt::time::sleep(REDIAL_SETTLE).await;
        }
    });
}

impl Subsystem for SyncWire {
    const NAME: &'static str = "sync/wire";
    const GENERATOR: u32 = 2;
    const SCENARIO_BYTES: usize = 2;
    type Scenario = SyncWireScenario;
    type Prepared = SyncPorts;

    /// [`wire_config`].
    fn config(scenario: &SyncWireScenario) -> RunConfig {
        wire_config(scenario)
    }

    /// `sync/storage`'s storages ([`SyncStorage::prepare`]).
    fn prepare(scenario: &SyncWireScenario) -> SyncPorts {
        SyncStorage::prepare(&scenario.sync)
    }

    /// Adds the network, spawns `sync/storage`'s actors, puts each behind a
    /// handler on its own endpoint, the node's tapped, and starts each
    /// connection's dialer. Then adds the `Control` source.
    fn build(sim: &mut Sim, scenario: &SyncWireScenario, ports: SyncPorts) -> Self {
        let net = scenario.net.net();
        sim.add_source(Box::new(net.clone()));
        let (actors, predicates) = spawn_actors(sim, &scenario.sync, &ports);
        let view = actors.view.clone();
        let header = actors.header.clone();
        let names: Vec<String> = (0..scenario.sync.mirrors.len())
            .map(|index| format!("mirror-{index}"))
            .collect();
        let (observer, tap) = TransportTap::new(sim, "node", net.clone(), names);
        let dials = tap.dials();
        sim.add_source(Box::new(tap));
        let network = ports.fixture.headers().network().clone();
        let handshake = ZakuraHandshakeConfig::for_network(&network);
        let seed = scenario.net.seed;
        let quic = QuicLinks {
            supervisor: Arc::default(),
            stopped: Arc::default(),
            peers: (0..scenario.sync.mirrors.len())
                .map(|index| (format!("mirror-{index}"), peer_id(seed, index)))
                .collect(),
        };
        let mut endpoints = Vec::new();
        for (index, node) in actors.nodes.iter().enumerate() {
            // The node is endpoint 0; mirror `i` is peer `i`.
            let peer = index.checked_sub(1);
            let (key, addr) =
                peer.map_or((0, NODE_ADDR), |peer| (key_index(peer), peer_addr(peer, 0)));
            let services = Services {
                header: Arc::new(HeaderSyncService::new(node.header.handle.clone())),
                block: node
                    .block
                    .wire_service()
                    .expect("a node just spawned holds its service"),
                key,
                addr,
            };
            let slot = (index == 0).then(|| quic.supervisor.clone());
            let (observer, dials): (Arc<dyn TransportObserver>, _) = if index == 0 {
                (observer.clone(), Some(dials.clone()))
            } else {
                (Arc::new(Unobserved), None)
            };
            endpoints.push(WireEndpoint::spawn_at(
                sim,
                &network,
                net.sim(),
                seed,
                peer,
                observer,
                dials,
                move |supervisor, limits| {
                    if let Some(slot) = slot {
                        *slot.lock().unwrap_or_else(PoisonError::into_inner) =
                            Some(supervisor.clone());
                    }
                    services.register(seed, &handshake, supervisor, limits)
                },
                |_, _, discovery| async move { discovery },
            ));
        }
        for (index, mirror) in scenario.sync.mirrors.iter().enumerate() {
            let node_target =
                NodeAddr::with_addrs(SeededInputs::node_key(seed, 0).public(), [NODE_ADDR]);
            let mirror_target = NodeAddr::with_addrs(
                SeededInputs::node_key(seed, key_index(index)).public(),
                [peer_addr(index, 0)],
            );
            let (node, mirror_end) = (endpoints[0].bound(), endpoints[index + 1].bound());
            match mirror.dialer {
                Dialer::Mirror => redial(sim, mirror_end, node, node_target, quic.stopped.clone()),
                Dialer::Node => redial(sim, node, mirror_end, mirror_target, quic.stopped.clone()),
            }
        }
        let topology = Rc::new(SyncTopology::new(
            actors,
            &scenario.sync,
            &ports,
            Transport::Quic(quic),
        ));
        let control = Control::new(
            sim,
            topology.clone(),
            scenario.sync.control.clone(),
            predicates,
        );
        sim.add_source(Box::new(control));
        SyncWire {
            view: SyncView {
                block: view,
                cross: ports.cross,
                header,
            },
            replies: ports.replies,
            _topology: topology,
            _endpoints: endpoints,
        }
    }

    /// `sync/storage`'s properties ([`sync_properties`]), then the session
    /// properties the shared connection reaches.
    fn properties() -> Vec<Property<Self>> {
        let mut properties = sync_properties();
        properties.extend(session::wire_properties());
        properties
    }

    /// `sync/storage`'s sources ([`storage_sources`](super::block_sync::storage_sources))
    /// but frames and closes: the node's frames
    /// cross QUIC, which the transport tap reports instead, and no
    /// in-memory reader records a close cause. The network's datagrams
    /// too.
    fn sources() -> Sources {
        Sources::observed(&[
            Source::Schedule,
            Source::Storage,
            Source::View,
            Source::Datagrams,
        ])
        .excluding(
            Source::Frames,
            "the wire tier's frames cross QUIC, and the transport tap reports them",
        )
        .excluding(
            Source::Closes,
            "the wire tier has no in-memory reader to record a close cause",
        )
    }

    /// The saved scenario `prefix` tags ([`SavedWire::from_tag`]), or the
    /// scenario [`combined`] decodes from it.
    fn draw(prefix: &[u8]) -> SyncWireScenario {
        SavedWire::from_tag(prefix).map_or_else(|| combined(prefix), SavedWire::scenario)
    }
}

/// A saved scenario of `sync/wire`. Each has a committed input under
/// `tests/fixtures/sync-wire-<name>.bin`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SavedWire {
    /// Eight blocks from two mirrors, one dialing each way.
    Honest,
    /// [`SavedWire::Honest`], with the node's connection to the mirror it
    /// dials closed once the node has asked storage to admit four bodies.
    /// The node redials it, so header sync, block sync and discovery each
    /// open a session on the replacement while the other mirror's
    /// connection stays up.
    Replace,
    /// [`SavedWire::Honest`], with every mirror's block-sync range read
    /// held for [`STALL_MS`]. Block sync's sessions stall with requests
    /// outstanding, while header sync and discovery share their
    /// connections. Once the first read is held, every mirror's chain grows
    /// by [`STALL_GROWTH`] blocks, so header sync has new tips to apply
    /// while block sync waits.
    StalledBlocks,
}

/// How long [`SavedWire::StalledBlocks`] holds each mirror read, in
/// milliseconds of virtual time: under block sync's 8-second request
/// timeout, so the stalled requests complete rather than time out.
pub const STALL_MS: u64 = 5_000;

/// How many blocks [`SavedWire::StalledBlocks`] grows the mirrors' chains
/// by while the first read is held.
pub const STALL_GROWTH: u32 = 4;

/// The first byte of a saved scenario's tag.
const TAG: u8 = 0xFC;

impl SavedWire {
    /// Every saved scenario, in tag order.
    pub const ALL: [SavedWire; 3] = [
        SavedWire::Honest,
        SavedWire::Replace,
        SavedWire::StalledBlocks,
    ];

    /// The fixture's name, `sync-wire-<name>.bin`.
    pub fn name(self) -> &'static str {
        match self {
            SavedWire::Honest => "honest",
            SavedWire::Replace => "replace",
            SavedWire::StalledBlocks => "stalled-blocks",
        }
    }

    /// The two-byte prefix that picks it: `0xFC`, then its place in
    /// [`SavedWire::ALL`] from 1.
    pub fn tag(self) -> [u8; 2] {
        let index = SavedWire::ALL
            .iter()
            .position(|saved| *saved == self)
            .expect("every saved scenario is listed");
        // A handful of scenarios fit a byte.
        [TAG, (index + 1) as u8]
    }

    /// The saved scenario `prefix` tags, if any.
    pub fn from_tag(prefix: &[u8]) -> Option<SavedWire> {
        SavedWire::ALL
            .into_iter()
            .find(|saved| prefix.get(..2) == Some(&saved.tag()[..]))
    }

    /// The properties whose trigger history its committed input reaches,
    /// each with the outcome the unpatched run gives, for its sidecar's
    /// `[[witness]]` entries.
    pub fn witnesses(self) -> &'static [(&'static str, &'static str)] {
        match self {
            SavedWire::Honest => &[],
            SavedWire::Replace => &[(session::REPLACEMENT_PRESERVES_AUTHORIZATION, "holds")],
            SavedWire::StalledBlocks => &[(session::STALLED_SERVICE_ISOLATED, "holds")],
        }
    }

    /// The scenario of an untagged input: [`SavedWire::Honest`]'s.
    pub fn default_scenario() -> SyncWireScenario {
        SavedWire::Honest.scenario()
    }

    /// Its scenario.
    pub fn scenario(self) -> SyncWireScenario {
        let sync = SyncScenario {
            mirrors: vec![
                SyncMirror {
                    dialer: Dialer::Mirror,
                },
                SyncMirror {
                    dialer: Dialer::Node,
                },
            ],
            ..SyncScenario::default()
        };
        let sync = match self {
            SavedWire::Honest => sync,
            SavedWire::Replace => SyncScenario {
                control: vec![SyncStep {
                    action: SyncAction::Close {
                        peer: "mirror-1".to_owned(),
                    },
                    when: Some(super::sync::admitted(4)),
                }],
                ..sync
            },
            SavedWire::StalledBlocks => {
                let read = block_policy::Select {
                    kind: Some(block_policy::BlockKind::MirrorRead),
                    ..block_policy::Select::default()
                };
                let mut block_policy = sync.block_policy.clone();
                block_policy.holds.push(block_policy::Hold {
                    select: read.clone(),
                    until: Trigger::VirtualTime(STALL_MS),
                    then: vec![block_policy::BlockAllow::Serve],
                    deadline_ms: None,
                });
                SyncScenario {
                    block_policy,
                    control: vec![SyncStep {
                        action: SyncAction::Grow { len: STALL_GROWTH },
                        when: Some(Trigger::Submitted(read)),
                    }],
                    ..sync
                }
            }
        };
        SyncWireScenario {
            net: NetScenario::default(),
            sync,
        }
    }
}

/// Curated combinations for the `sync_wire` fuzz target's seeds, each a
/// [`COMBINED_TAG`] prefix of [`combined`]'s actions, by name. Each has a
/// committed input under `tests/fixtures/sync-wire-<name>.bin`.
pub const CURATED: [(&str, [u8; 2]); 4] = [
    // The fork while storage holds a submission, and the node's
    // connection to mirror-1 replaced once storage admits two bodies.
    ("combined-reorg-submit-replace", [COMBINED_TAG, 0b0110_1100]),
    // The fork while storage holds a needed-block query, and mirror-0's
    // connection replaced once storage admits one body.
    ("combined-reorg-query-replace", [COMBINED_TAG, 0b0000_1010]),
    // Mirror-0's connection replaced once storage admits one body, with
    // every read stalled.
    ("combined-replace-stall", [COMBINED_TAG, 0b0001_1000]),
    // The fork while storage holds a mirror's range read, with every read
    // stalled. A late body for a retired request answers the height another
    // connection's request still claims.
    ("combined-reorg-request-stall", [COMBINED_TAG, 0b0001_0011]),
];

/// The first byte of a prefix that picks fuzz-13's combined actions
/// ([`combined`]). Other untagged prefixes pick [`SavedWire::Honest`], so a
/// campaign's random inputs stay on the honest scenario's schedule.
pub const COMBINED_TAG: u8 = 0xFB;

/// The scenario a prefix picks when it tags no saved scenario: fuzz-13's
/// combined actions on [`SavedWire::Honest`]'s mirrors if its first byte is
/// [`COMBINED_TAG`], or [`SavedWire::Honest`]'s scenario. The second
/// byte's bits pick the actions and their parameters, from the low bit:
///
/// - Bits 0–2 pick a chain change: none, the mirrors' fork once storage
///   admits four bodies, or the fork while storage holds the node's work at
///   one stage of the block pipeline, as `sync/storage`'s saved reorg
///   scenarios hold it ([`SavedSync`]): a needed-block query, a mirror's
///   range read, a submission, or a verdict. Values past those pick none.
/// - Bit 3 replaces a connection: it closes the node's connection to the
///   mirror bit 5 names once storage admits one to four bodies, as bits 6–7
///   say. The dialer redials it.
/// - Bit 4 stalls every mirror's range reads for [`STALL_MS`].
///
/// A service stream can't be replaced on its own connection: no `Control`
/// action reaches a single stream on QUIC, so connection replacement stands
/// in for it.
pub fn combined(prefix: &[u8]) -> SyncWireScenario {
    let actions = match prefix {
        [COMBINED_TAG, actions, ..] => *actions,
        _ => return SavedWire::Honest.scenario(),
    };
    let mut sync = match actions & 0b111 {
        1 => SavedSync::Reorg,
        2 => SavedSync::ReorgQuery,
        3 => SavedSync::ReorgRequest,
        4 => SavedSync::ReorgSubmit,
        5 => SavedSync::ReorgVerdict,
        _ => SavedSync::Honest,
    }
    .scenario();
    if actions & 0b1000 != 0 {
        let mirror = (actions >> 5) & 1;
        let admitted = 1 + u32::from(actions >> 6);
        sync.control.push(SyncStep {
            action: SyncAction::Close {
                peer: format!("mirror-{mirror}"),
            },
            when: Some(super::sync::admitted(admitted)),
        });
    }
    if actions & 0b1_0000 != 0 {
        sync.block_policy.holds.push(block_policy::Hold {
            select: block_policy::Select {
                kind: Some(block_policy::BlockKind::MirrorRead),
                ..block_policy::Select::default()
            },
            until: Trigger::VirtualTime(STALL_MS),
            then: vec![block_policy::BlockAllow::Serve],
            deadline_ms: None,
        });
    }
    SyncWireScenario {
        net: NetScenario::default(),
        sync,
    }
}
