//! `sync/wire` (xsync-3): `sync/storage`'s node and mirrors, each behind a
//! real `ZakuraProtocolHandler` on its own `zakura-quic` endpoint, bound to
//! the run's `SimNet`. Each handler registers header sync, block sync and
//! discovery, so every connection carries all three services' streams, as
//! a production node's does.
//!
//! The node's actors, taps and engine watch are `sync/storage`'s
//! ([`spawn_actors`]). The node's transport is tapped ([`TransportTap`]).
//! A mirror's transport reports to no one, unless the scenario sets QUIC
//! windows ([`WireWindows`], sim-9.3): then every mirror's transport is
//! tapped too, and every tap samples each connection's resources after
//! every step for the connection properties
//! ([`connection`](crate::domain::connection)).
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
    cell::{Cell, RefCell},
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, PoisonError,
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use zakura_network::zakura::{
    memory::MemoryPool, testkit::discovery::build_discovery_handle, DiscoveryService,
    HeaderSyncService, Service, TransportEvent, TransportObserver, ZakuraHandshakeConfig,
    ZakuraPeerId, ZakuraServiceId,
};
use zakura_quic::{sim::SeededInputs, NodeAddr};
use zakura_rt::sync::watch;
use zakura_scheduler::{At, SimInstant, TaskId};

use super::{
    block_node::{
        control::Fork,
        policy::{self as block_policy, Predicates, Trigger},
        tap::TapRoutines,
        view::BlockSyncView,
        LateHandle,
    },
    header_sync::Dialer,
    policy::LinkEffect,
    sync::{
        spawn_actors_with_memory, sync_config, sync_properties, Control, MirrorServices, QuicLinks,
        SavedSync, SyncAction, SyncMirror, SyncPorts, SyncScenario, SyncStep, SyncStorage,
        SyncTopology, SyncView, Transport,
    },
    wire_node::config as wire_config_for,
    wire_node::{Bound, TransportTap, WireEndpoint},
    LinkSpec, NetScenario, ScenarioFamily, Subsystem,
};
use crate::{
    domain::{
        connection::{self, WireBounds, WireState},
        header_sync::{
            teardown_end_violations as header_teardown_end_violations, TeardownView as _,
        },
        memory_pool, session,
    },
    observe::TransportFact,
    peer::{key_index, peer_addr, peer_id, peer_of, NODE_ADDR},
    property::{Capture, HasView, Property, PropertyId, Source, Sources},
    witness::Expect,
    ActionSource, Boundary, Identity, Observation, ObservationLog, RecoveryOffer, RunConfig, Sim,
    TaskHold,
};

/// How long a dialer waits after its connection ends before it dials
/// again: the production handshake timeout, so the node has torn the old
/// connection down and doesn't refuse the new one as a duplicate.
pub const REDIAL_SETTLE: Duration = Duration::from_secs(10);

/// The most connections one dialer makes over a run.
const MAX_INCARNATIONS: u32 = 4;

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
    /// The QUIC windows the node and the mirrors run with in place of
    /// production's, which also taps every mirror's transport and samples
    /// every connection's resources (sim-9.3). `None` runs production's
    /// windows with only the node's transport tapped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub windows: Option<WireWindows>,
    /// Holds the block-sync routine that a `Control` close supersedes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_hold: Option<SupersededHold>,
    /// Optional logical P2P memory budget `B` in bytes for the node's memory
    /// pool (`None` uses the simulator default of 16 GiB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_budget_bytes: Option<u64>,
}

/// The QUIC windows a sim-9.3 scenario sets. The node's reactor shares,
/// which its connections add to their receive windows, stay production's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireWindows {
    /// The node's `[network.zakura.quic] receive_window_bytes`: the
    /// handshake share every connection keeps beside its kinds' shares.
    pub node_receive_window_bytes: u32,
    /// Every mirror's `[network.zakura.quic] send_window_bytes`: the most
    /// unacknowledged stream bytes it keeps in flight and buffered on one
    /// connection.
    pub mirror_send_window_bytes: u64,
}

/// Holds the node's block-sync routine on the first connection a `Control`
/// close ends, from the close until block sync admits the peer's next
/// session, as a busy thread would leave the superseded routine's drop
/// behind its replacement's admission. The routine's drop then makes its
/// generation-gated registry writes against the replacement's entry.
///
/// The routine still runs while another task is parked on a lock it holds.
/// The hold ends, for good, at that admission, or, with `until_claimed`,
/// once the replacement's routine reports a published claim, or `steps`
/// steps after the close. The routine also runs whenever nothing else can,
/// since a [`TaskHold`] never ends a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupersededHold {
    /// The most steps the hold lasts after the close.
    pub steps: u64,
    /// Whether the hold lasts past the replacement's admission until the
    /// replacement's routine reports a published claim, so the superseded
    /// routine's drop meets an entry with claims.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub until_claimed: bool,
}

/// [`SupersededHold`]'s hold: the tasks of the closed peer's routines that
/// block sync admitted before the close.
struct HeldSuperseded {
    routines: TapRoutines,
    /// The peers `Control` closed, in close order.
    closed: Rc<RefCell<Vec<ZakuraPeerId>>>,
    view: Rc<RefCell<BlockSyncView>>,
    steps: u64,
    until_claimed: bool,
    /// The closed peer, its newest generation at the close, and the step
    /// after which the hold began, once a close happened.
    armed: RefCell<Option<(ZakuraPeerId, u64, u64)>>,
    /// Set once the hold ends.
    over: Cell<bool>,
}

impl TaskHold for HeldSuperseded {
    fn holds(&self, task: TaskId, _at: &At) -> bool {
        if self.over.get() {
            return false;
        }
        let step = self.view.borrow().step.unwrap_or(0);
        let mut armed = self.armed.borrow_mut();
        if armed.is_none() {
            let Some(peer) = self.closed.borrow().first().cloned() else {
                return false;
            };
            let Some(generation) = self.routines.admitted(&peer) else {
                self.over.set(true);
                return false;
            };
            *armed = Some((peer, generation, step));
        }
        let (peer, superseded, since) = armed.as_ref().expect("the hold armed above");
        let replaced = if self.until_claimed {
            self.view.borrow().routines.values().any(|routine| {
                routine.peer == *peer
                    && routine.generation > *superseded
                    && !routine.claims.is_empty()
            })
        } else {
            self.routines
                .admitted(peer)
                .is_some_and(|newest| newest > *superseded)
        };
        if replaced || step >= since.saturating_add(self.steps) {
            self.over.set(true);
            return false;
        }
        self.routines
            .routine(task)
            .is_some_and(|(routine, generation)| routine == *peer && generation <= *superseded)
    }

    /// The routine runs while a task is parked on a lock it holds. Held
    /// inside its writer fence's lock, it would keep the closed connection's
    /// teardown from taking the fence, so the redial would never start and
    /// the clock would run on until the other connections idle out.
    fn holds_while_waited_on(&self) -> bool {
        false
    }
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
    /// Keeps the node's memory pool (and its observer) alive across teardown.
    _memory_pool: MemoryPool,
}

impl HasView for SyncWire {
    type View = SyncView;

    /// Drops the node's block-sync handle from its block storage's
    /// replies. The handle reaches the storage again through the reactor's
    /// range source, so the cycle would outlive the run. Records the run's
    /// end in header sync's round evidence, for the teardown rule.
    fn ended(&self, end: &crate::RunEnd) {
        let _ = self.replies.clear();
        self.view.rounds().end(end);
    }

    /// [`header_teardown_end_violations`].
    fn end_violations(&self, _: &crate::ObservationLog) -> Vec<crate::property::EndViolation> {
        header_teardown_end_violations(self.view.rounds())
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

/// Counts each connection the node registers as a link to the mirror it
/// comes from, for the block policy's [`Trigger::Linked`]. A wire run's
/// dialers make its connections, so no `sync/storage` link records them.
/// It's added after the node's tap, so a connection counts in the step the
/// tap logs its registration.
struct RegisteredLinks {
    predicates: Rc<RefCell<Predicates>>,
    /// Each mirror's name, by index.
    names: Vec<String>,
    /// The log entries already read.
    seen: usize,
}

impl ActionSource for RegisteredLinks {
    /// Nothing.
    fn enabled(&self, _: SimInstant, _: &mut Vec<(Identity, u64)>) {}

    /// Nothing.
    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    /// Never called, since it offers nothing.
    fn execute(&mut self, _: u64, _: &mut ObservationLog) {
        unreachable!("the link count offers no action")
    }

    /// Records a [`LinkEffect::Linked`] for each `registered` row logged
    /// since the last observation. A row from no mirror's address fails the
    /// run.
    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        let entries = log.entries();
        for entry in &entries[self.seen..] {
            if let Observation::Transport {
                remote,
                event: TransportFact::Registered,
                ..
            } = &entry.obs
            {
                let name = remote
                    .parse()
                    .ok()
                    .and_then(peer_of)
                    .and_then(|index| self.names.get(index))
                    .ok_or_else(|| format!("AmbiguousIdentity: no mirror binds {remote}"))?;
                self.predicates
                    .borrow_mut()
                    .record_link(LinkEffect::Linked, name);
            }
        }
        self.seen = entries.len();
        Ok(())
    }

    /// Nothing: it finishes no submitted work.
    fn recovery(&self, _now: SimInstant, _out: &mut RecoveryOffer) {}

    /// Nothing: it has no holds.
    fn enter_drain(&mut self, _log: &mut ObservationLog) {}

    /// None: it holds no submitted work.
    fn holds_work(&self) -> Option<String> {
        None
    }
}

/// Live steps a wire run allows per fixture block, past
/// [`sync_config`]'s: each frame crosses QUIC's handshakes, acknowledgements
/// and timers.
const WIRE_STEPS_PER_BLOCK: u64 = 6_000;

/// The node's and every mirror's network config: production defaults on
/// `network`, with `windows`' QUIC windows if set.
fn configs(
    network: &zakura_chain::parameters::Network,
    windows: Option<WireWindows>,
) -> (zakura_network::Config, zakura_network::Config) {
    let (mut node, mut mirror) = (wire_config_for(network), wire_config_for(network));
    if let Some(windows) = windows {
        node.zakura.quic.receive_window_bytes = Some(windows.node_receive_window_bytes);
        mirror.zakura.quic.send_window_bytes = windows.mirror_send_window_bytes;
    }
    (node, mirror)
}

/// [`sync_config`], with room for QUIC's own steps for each fixture block
/// and each block a `Control` step grows or forks, and the harness timer
/// cap: a peer asleep in its redial settle at the budget is a bounded tail
/// the drain must let finish ([`crate::sim::HARNESS_TIMER_CAP`]).
pub fn wire_config(scenario: &SyncWireScenario) -> RunConfig {
    let config = sync_config(&scenario.sync);
    let blocks = u64::from(scenario.sync.blocks) + scenario.sync.changed();
    RunConfig {
        budget: scenario
            .sync
            .steps
            .unwrap_or(40_000 + WIRE_STEPS_PER_BLOCK * blocks),
        harness_timer_cap: crate::sim::HARNESS_TIMER_CAP,
        ..config
    }
}

/// What one actor's handler registers: its header and block services and
/// a discovery service, keyed by the actor's endpoint key.
struct Services {
    header: Arc<dyn Service>,
    block: Arc<dyn Service>,
    reactors: MirrorServices,
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
        let advertised = match self.reactors {
            MirrorServices::DiscoveryOnly => vec![ZakuraServiceId::discovery()],
            MirrorServices::HeaderSync | MirrorServices::All => {
                vec![ZakuraServiceId::discovery(), ZakuraServiceId::header_sync()]
            }
        };
        let discovery = build_discovery_handle(
            SeededInputs::node_key(seed, self.key),
            vec![self.addr],
            advertised,
            handshake,
            limits.max_connections,
            0,
            supervisor.subscribe(),
        )
        .expect("production's config on the fixture network is valid");
        let (owners, mut registered) = match self.reactors {
            MirrorServices::DiscoveryOnly => (Vec::new(), Vec::new()),
            MirrorServices::HeaderSync => (vec![self.header.clone()], vec![self.header]),
            MirrorServices::All => (
                vec![self.header.clone(), self.block.clone()],
                vec![self.header, self.block],
            ),
        };
        let service: Arc<dyn Service> = Arc::new(
            DiscoveryService::with_max_connections(discovery.clone(), limits.max_connections)
                .with_connection_owners(owners),
        );
        registered.push(service);
        (registered, discovery)
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

/// The tag of `sync/wire`'s free family, which reads a `sync/composed`
/// script ([`sync_composed`]) from the prefix's next bytes.
pub const WIRE_COMPOSED_TAG: u8 = 0xFA;

/// `sync/wire`'s prefix before its free family: a saved scenario's tag, or
/// [`COMBINED_TAG`] and its actions byte.
const WIRE_PLANNED_BYTES: usize = 2;

impl Subsystem for SyncWire {
    const NAME: &'static str = "sync/wire";
    const GENERATOR: u32 = 6;
    /// [`WIRE_COMPOSED_TAG`], then a `sync/composed` script in the place of
    /// its version byte and the bytes after it.
    const SCENARIO_BYTES: usize = SYNC_COMPOSED_BYTES;
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
    /// handler on its own endpoint, the node's tapped and its connections
    /// counted as links ([`RegisteredLinks`]), and starts each connection's
    /// dialer. Then adds the `Control` source.
    fn build(sim: &mut Sim, scenario: &SyncWireScenario, ports: SyncPorts) -> Self {
        let net = scenario.net.net();
        sim.add_source(Box::new(net.clone()));
        let network = ports.fixture.headers().network().clone();
        let (node_config, mirror_config) = configs(&network, scenario.windows);
        let (actors, predicates, pool_tap) = spawn_actors_with_memory(
            sim,
            &scenario.sync,
            &ports,
            &node_config,
            scenario.memory_budget_bytes,
        );
        let view = actors.view.clone();
        let header = actors.header.clone();
        let memory_plan = actors.memory_plan.clone();
        let memory_pool = actors.memory_pool.clone();
        let serving_pool = actors.serving_pool.clone();
        let names: Vec<String> = (0..scenario.sync.mirrors.len())
            .map(|index| format!("mirror-{index}"))
            .collect();
        let (observer, tap) = match scenario.windows {
            Some(_) => TransportTap::sampling(sim, "node", net.clone(), names.clone()),
            None => TransportTap::new(sim, "node", net.clone(), names.clone()),
        };
        let dials = tap.dials();
        sim.add_source(Box::new(tap));
        sim.add_source(Box::new(pool_tap));
        sim.add_source(Box::new(RegisteredLinks {
            predicates: predicates.clone(),
            names: names.clone(),
            seen: 0,
        }));
        let mut mirror_observers = Vec::new();
        if scenario.windows.is_some() {
            for name in &names {
                let (observer, tap) = TransportTap::of_peer(sim, name, net.clone());
                sim.add_source(Box::new(tap));
                mirror_observers.push(observer);
            }
        }
        let wire = scenario.windows.map(|_| {
            Rc::new(WireState::new(WireBounds {
                node: memory_plan.local_limits(&node_config),
                peer_send_window: mirror_config.zakura.quic.send_window_bytes,
            }))
        });
        let handshake = ZakuraHandshakeConfig::for_network(&network);
        let seed = scenario.net.seed;
        let quic = QuicLinks {
            supervisor: Arc::default(),
            stopped: Arc::default(),
            peers: (0..scenario.sync.mirrors.len())
                .map(|index| (format!("mirror-{index}"), peer_id(seed, index)))
                .collect(),
            closed: Rc::default(),
        };
        if let Some(hold) = scenario.superseded_hold {
            sim.hold_tasks(Box::new(HeldSuperseded {
                routines: actors.routines.clone(),
                closed: quic.closed.clone(),
                view: view.clone(),
                steps: hold.steps,
                until_claimed: hold.until_claimed,
                armed: RefCell::default(),
                over: Cell::new(false),
            }));
        }
        let mut endpoints = Vec::new();
        for (index, node) in actors.nodes.iter().enumerate() {
            // The node is endpoint 0; mirror `i` is peer `i`.
            let peer = index.checked_sub(1);
            let (key, addr) =
                peer.map_or((0, NODE_ADDR), |peer| (key_index(peer), peer_addr(peer, 0)));
            let reactors = peer
                .and_then(|peer| scenario.sync.mirrors[peer].services)
                .unwrap_or(MirrorServices::All);
            let services = Services {
                header: Arc::new(HeaderSyncService::new(node.header.handle.clone())),
                block: node
                    .block
                    .wire_service()
                    .expect("a node just spawned holds its service"),
                reactors,
                key,
                addr,
            };
            let slot = (index == 0).then(|| quic.supervisor.clone());
            let (observer, dials, config, memory): (Arc<dyn TransportObserver>, _, _, _) =
                match peer {
                    None => (
                        observer.clone(),
                        Some(dials.clone()),
                        node_config.clone(),
                        Some((
                            memory_plan.clone(),
                            memory_pool.clone(),
                            serving_pool.clone(),
                        )),
                    ),
                    Some(peer) => (
                        mirror_observers
                            .get(peer)
                            .cloned()
                            .unwrap_or_else(|| Arc::new(Unobserved)),
                        None,
                        mirror_config.clone(),
                        None,
                    ),
                };
            endpoints.push(WireEndpoint::spawn_with_memory(
                sim,
                config,
                net.sim(),
                seed,
                peer,
                None,
                observer,
                dials,
                memory,
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
                wire,
            },
            replies: ports.replies,
            _topology: topology,
            _endpoints: endpoints,
            _memory_pool: memory_pool,
        }
    }

    /// [`wire_harness_properties`].
    fn properties() -> Vec<Property<Self>> {
        wire_harness_properties()
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

    /// Each saved scenario's tag ([`SavedWire::tag`]), and
    /// [`COMBINED_TAG`], whose next byte picks the combined actions.
    /// [`SavedWire::Honest`]'s tag draws the untagged scenario, so it isn't
    /// a family.
    fn families() -> Vec<ScenarioFamily> {
        let mut families: Vec<ScenarioFamily> = SavedWire::ALL
            .into_iter()
            .filter(|saved| *saved != SavedWire::Honest)
            .map(|saved| ScenarioFamily::new(saved.name(), saved.tag()))
            .collect();
        families.push(ScenarioFamily::new("combined", [COMBINED_TAG]));
        families.push(ScenarioFamily::free("composed", [WIRE_COMPOSED_TAG]));
        families
    }

    /// The script [`WIRE_COMPOSED_TAG`] composes from the bytes after it
    /// ([`sync_composed`]), the saved scenario `prefix` tags
    /// ([`SavedWire::from_tag`]), or the scenario [`combined`] decodes
    /// from it. So a planned family and a free script share one input
    /// format, and a fuzzer's mutation of a tag moves between them.
    fn draw(prefix: &[u8]) -> SyncWireScenario {
        if let Some((&WIRE_COMPOSED_TAG, script)) = prefix.split_first() {
            return sync_composed(script);
        }
        SavedWire::from_tag(prefix).map_or_else(|| combined(prefix), SavedWire::scenario)
    }

    /// Generator 5's input, whose prefix held only two bytes, with the
    /// prefix widened to [`Subsystem::SCENARIO_BYTES`]. No generator-5
    /// prefix starts with [`WIRE_COMPOSED_TAG`] but the honest scenario's,
    /// which [`combined`] drew for any untagged first byte, so the migrated
    /// input reads it as the honest scenario's tag instead.
    fn migrate(source: &[u8], generator: u32) -> Result<Vec<u8>, String> {
        match generator {
            5 => {
                let mut widened =
                    super::widen_prefix(source, WIRE_PLANNED_BYTES, Self::SCENARIO_BYTES);
                if widened.first() == Some(&WIRE_COMPOSED_TAG) {
                    widened[..WIRE_PLANNED_BYTES].copy_from_slice(&SavedWire::Honest.tag());
                }
                Ok(widened)
            }
            _ => Err(format!(
                "{} migrates no generator-{generator} input",
                Self::NAME
            )),
        }
    }
}

/// A saved scenario of `sync/wire`. Each is the seed `sync-wire-<name>`:
/// its tag and the empty Plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SavedWire {
    /// Eight blocks from two mirrors, one dialing each way.
    Honest,
    /// [`SavedWire::Honest`], with the node's connection to the mirror it
    /// dials closed once the node has asked storage to admit four bodies.
    /// The node redials it, so header sync, block sync and discovery each
    /// open a session on the replacement while the other mirror's
    /// connection stays up. A [`SupersededHold`] keeps the closed
    /// connection's block-sync routine from running until block sync admits
    /// the replacement's session, so the routine's drop writes to the peer
    /// registry after that admission.
    Replace,
    /// [`SavedWire::Honest`], with every mirror's block-sync range read
    /// held for [`STALL_MS`]. Block sync's sessions stall with requests
    /// outstanding, while header sync and discovery share their
    /// connections. Once the first read is held, every mirror's chain grows
    /// by [`STALL_GROWTH`] blocks, so header sync has new tips to apply
    /// while block sync waits.
    StalledBlocks,
    /// sim-9.3's saturating peers: [`SavedWire::Honest`]'s mirrors serve
    /// [`SATURATED_BODY_BYTES`]-byte bodies through a QUIC send window of
    /// [`SATURATED_WINDOW_BYTES`], so each mirror's block-sync writes keep
    /// its send path full, and the node keeps a handshake share of the same
    /// size, so its held block frames take more than that share. Once the
    /// first range read is submitted, every mirror's chain grows by
    /// [`STALL_GROWTH`] blocks, so the mirrors write header-sync pages while
    /// their block-sync writes saturate.
    Saturated,
    /// [`SavedWire::Replace`] with one mirror, which the node dials: the
    /// node's connection to it closes once the node has asked storage to
    /// admit four bodies, and the node redials it. The mirror's chain grows
    /// by [`REPLACE_GROWTH`] blocks [`REPLACE_GROWTH_MS`] into the run,
    /// after the redial, so only the replacement's routine can request the
    /// new bodies. The [`SupersededHold`] lasts until that routine reports a
    /// published claim, so the superseded routine's drop meets an entry
    /// with claims, and a write that clears them by peer ID alone changes
    /// the entry.
    ReplaceClaims,
}

/// Each body's size in [`SavedWire::Saturated`]: a quarter of the largest
/// block, so one body takes several send windows.
pub const SATURATED_BODY_BYTES: u32 = 500_000;

/// Every link's one-way delay in [`SavedWire::Saturated`], in
/// milliseconds: each send window takes a round trip, so a body takes some
/// eight round trips, and the transfers span the header-sync and
/// discovery frames of the next seconds.
pub const SATURATED_DELAY_MS: u64 = 50;

/// [`SavedWire::Saturated`]'s step budget: QUIC moves each body in some
/// ten thousand steps, past [`wire_config`]'s allowance for small bodies.
pub const SATURATED_STEPS: u64 = 400_000;

/// The node's handshake share and every mirror's QUIC send window in
/// [`SavedWire::Saturated`]: the smallest window `QuicConfig::validate`
/// accepts.
pub const SATURATED_WINDOW_BYTES: u32 = 64 * 1024;

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
    pub const ALL: [SavedWire; 5] = [
        SavedWire::Honest,
        SavedWire::Replace,
        SavedWire::StalledBlocks,
        SavedWire::Saturated,
        SavedWire::ReplaceClaims,
    ];

    /// Its name, which names its seed `sync-wire-<name>`.
    pub fn name(self) -> &'static str {
        match self {
            SavedWire::Honest => "honest",
            SavedWire::Replace => "replace",
            SavedWire::StalledBlocks => "stalled-blocks",
            SavedWire::Saturated => "saturated",
            SavedWire::ReplaceClaims => "replace-claims",
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

    /// The properties whose trigger history its seed reaches, each with the
    /// outcome the unpatched run gives.
    pub fn witnesses(self) -> &'static [(&'static str, Expect)] {
        match self {
            SavedWire::Honest => &[],
            SavedWire::Replace | SavedWire::ReplaceClaims => {
                &[(session::REPLACEMENT_PRESERVES_AUTHORIZATION, Expect::Holds)]
            }
            SavedWire::StalledBlocks => &[(session::STALLED_SERVICE_ISOLATED, Expect::Holds)],
            SavedWire::Saturated => &[
                (connection::CREDIT_ISOLATED, Expect::Holds),
                (connection::SEND_ISOLATED, Expect::Holds),
                (connection::CONNECTION_SHARES_BOUND, Expect::Holds),
            ],
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
                    services: None,
                },
                SyncMirror {
                    dialer: Dialer::Node,
                    services: None,
                },
            ],
            ..SyncScenario::default()
        };
        let mut windows = None;
        let mut net = NetScenario::default();
        let sync = match self {
            SavedWire::Honest => sync,
            SavedWire::Saturated => {
                windows = Some(WireWindows {
                    node_receive_window_bytes: SATURATED_WINDOW_BYTES,
                    mirror_send_window_bytes: u64::from(SATURATED_WINDOW_BYTES),
                });
                net.default_link = LinkSpec {
                    min_delay_ms: SATURATED_DELAY_MS,
                    max_delay_ms: SATURATED_DELAY_MS,
                    ..LinkSpec::default()
                };
                SyncScenario {
                    body_bytes: SATURATED_BODY_BYTES,
                    steps: Some(SATURATED_STEPS),
                    control: vec![SyncStep {
                        action: SyncAction::Grow { len: STALL_GROWTH },
                        when: Some(Trigger::Submitted(block_policy::Select {
                            kind: Some(block_policy::BlockKind::MirrorRead),
                            ..block_policy::Select::default()
                        })),
                    }],
                    ..sync
                }
            }
            SavedWire::Replace => SyncScenario {
                control: vec![SyncStep {
                    action: SyncAction::Close {
                        peer: "mirror-1".to_owned(),
                    },
                    when: Some(super::sync::admitted(4)),
                }],
                ..sync
            },
            SavedWire::ReplaceClaims => SyncScenario {
                mirrors: vec![SyncMirror {
                    dialer: Dialer::Node,
                    services: None,
                }],
                control: vec![
                    SyncStep {
                        action: SyncAction::Close {
                            peer: "mirror-0".to_owned(),
                        },
                        when: Some(super::sync::admitted(4)),
                    },
                    SyncStep {
                        action: SyncAction::Grow {
                            len: REPLACE_GROWTH,
                        },
                        when: Some(Trigger::VirtualTime(REPLACE_GROWTH_MS)),
                    },
                ],
                ..sync
            },
            SavedWire::StalledBlocks => {
                let read = block_policy::Select {
                    kind: Some(block_policy::BlockKind::MirrorRead),
                    ..block_policy::Select::default()
                };
                let mut block_policy = sync.block_policy.clone();
                // Hold each read for 7.6 s (> STALL_MS = 5 s, < 8 s request timeout)
                // so the two stalled waves span discovery's 15 s periodic refresh
                // regardless of initial handshake ordering.
                block_policy.holds.push(block_policy::Hold {
                    select: read.clone(),
                    until: Trigger::VirtualTime(7_600),
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
        let superseded_hold = match self {
            SavedWire::Replace | SavedWire::ReplaceClaims => Some(SupersededHold {
                steps: REPLACE_HOLD_STEPS,
                until_claimed: self == SavedWire::ReplaceClaims,
            }),
            _ => None,
        };
        SyncWireScenario {
            net,
            sync,
            windows,
            superseded_hold,
            memory_budget_bytes: None,
        }
    }
}

/// How many steps [`SavedWire::Replace`] and [`SavedWire::ReplaceClaims`]
/// hold the superseded routine at most: past the redial, [`REDIAL_SETTLE`]
/// after the close, and past the replacement's first claim, which follows
/// the mirror's grown status 40 seconds into the run.
const REPLACE_HOLD_STEPS: u64 = 40_000;

/// When [`SavedWire::ReplaceClaims`] grows the mirror's chain, in
/// milliseconds of virtual time from the run's start: a second after the
/// redial, since the close comes before the clock first moves.
pub const REPLACE_GROWTH_MS: u64 = 11_000;

/// How many blocks [`SavedWire::ReplaceClaims`] grows the mirror's chain
/// by.
pub const REPLACE_GROWTH: u32 = 4;

/// Curated combinations for the `sync_wire` fuzz target's seeds, each a
/// [`COMBINED_TAG`] prefix of [`combined`]'s actions, by name. Each is the
/// seed `sync-wire-<name>`: its prefix and the empty Plan.
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
        windows: None,
        superseded_hold: None,
        memory_budget_bytes: None,
    }
}

/// [`sync_properties`] that the wire tier can run, then the session
/// properties the shared connection reaches, then the connection properties
/// a run with sampling taps reaches. A property that reads frames or closes,
/// which the wire tier excludes ([`SyncWire::sources`]), runs on
/// `block-sync/storage` and `sync/storage` instead, and so does
/// `memory/teardown-returns-zero`, since the wire tier's teardown isn't judged.
fn wire_harness_properties<S: HasView<View = SyncView>>() -> Vec<Property<S>> {
    let sources = SyncWire::sources();
    let mut properties: Vec<Property<S>> = sync_properties()
        .into_iter()
        .filter(|property| {
            sources.observes_all(property.needs)
                && property.id != PropertyId::new(memory_pool::TEARDOWN_RETURNS_ZERO)
        })
        .collect();
    properties.push(memory_pool::sessions_within_share());
    properties.extend(session::wire_properties());
    properties.extend(connection::wire_properties());
    properties
}

/// The event slots of a `sync/composed` prefix.
pub const SYNC_COMPOSED_SLOTS: usize = 8;

/// The first byte of every `sync/composed` prefix that [`sync_composed`]
/// reads: [`SyncComposed`]'s generator. Any other first byte draws
/// [`SavedWire::Honest`], as `header-sync/storage`'s grammar does.
pub const SYNC_COMPOSED_VERSION: u8 = 1;

/// The length of a `sync/composed` prefix: its version byte, a topology
/// byte, a chain byte, then three bytes per event slot.
pub const SYNC_COMPOSED_BYTES: usize = 3 + 3 * SYNC_COMPOSED_SLOTS;

/// How long a composed hold keeps each operation it holds, in milliseconds
/// after that operation's submission, as [`Trigger::VirtualTime`] counts in
/// a hold: from a short stall to past block sync's 8-second request timeout.
const HOLD_UNTIL_MS: [u64; 8] = [1_000, 3_000, 6_000, 9_000, 10_000, 12_000, 16_000, 24_000];

/// The scenario the bytes after a `sync/composed` prefix's version byte
/// encode, in fixed slots so a change to one byte changes one choice
/// (fuzz-11). Counting from the byte after the version:
///
/// - Byte 0, the topology: 1, 2, 2 or 3 mirrors (bits 0 and 1), whether
///   the node dials mirror `i` rather than the mirror dialing it (bit
///   `2 + i`), and the links' delays (bits 5 and 6: 0 to 100 ms, 1 to 5
///   ms, 50 to 400 ms, or 0 to 1,000 ms).
/// - Byte 1, the chain: 6 to 13 blocks (bits 0 to 2), of 2,000, 8,000,
///   32,000 or 64,000 bytes each (bits 3 and 4).
/// - Then [`SYNC_COMPOSED_SLOTS`] events of three bytes, a kind, `a` and
///   `b`, each read in order until one ends the list. By the kind's value
///   modulo 8: 0 ends it; 1 and 2 close mirror `a`'s connection, which its
///   dialer redials; 3 grows every mirror's chain by 1 to 4 blocks; 4
///   resets every mirror to a fork below the tip that ends above it; 5 and
///   6 hold block operations ([`composed_hold`]); 7 tears the node down
///   and ends the list. A control step's trigger comes from `b`
///   ([`composed_trigger`]), and control steps run in order.
///
/// A close waits until its mirror has one more link than it has had
/// composed closes, since no relink runs on the wire tier. A dialer makes
/// at most [`MAX_INCARNATIONS`] connections, so a mirror's closes past that
/// many are skipped: each would wait forever and hold back every later
/// step. A close is a [`SyncAction::CloseIfLinked`], since the node may
/// have closed the connection itself first.
pub fn sync_composed(grammar: &[u8]) -> SyncWireScenario {
    let byte = |index: usize| grammar.get(index).copied().unwrap_or(0);
    let (topology, chain) = (byte(0), byte(1));
    let mirrors = [1, 2, 2, 3][usize::from(topology & 3)];
    let mut net = NetScenario::default();
    (net.default_link.min_delay_ms, net.default_link.max_delay_ms) = match (topology >> 5) & 3 {
        0 => (0, 100),
        1 => (1, 5),
        2 => (50, 400),
        _ => (0, 1_000),
    };
    let blocks = 6 + u32::from(chain & 7);
    let mut sync = SyncScenario {
        blocks,
        body_bytes: [2_000, 8_000, 32_000, 64_000][usize::from((chain >> 3) & 3)],
        mirrors: (0..mirrors)
            .map(|index| SyncMirror {
                dialer: if topology & (4 << index) != 0 {
                    Dialer::Node
                } else {
                    Dialer::Mirror
                },
                services: None,
            })
            .collect(),
        ..SyncScenario::default()
    };
    let mut tip = blocks;
    let mut closes = vec![0u32; mirrors];
    let mut branch = 0u8;
    for slot in 0..SYNC_COMPOSED_SLOTS {
        let at = 2 + 3 * slot;
        let (kind, a, b) = (byte(at), byte(at + 1), byte(at + 2));
        let when = composed_trigger(b);
        match kind % 8 {
            0 => break,
            1 | 2 => {
                let mirror = usize::from(a) % mirrors;
                if closes[mirror] == MAX_INCARNATIONS {
                    continue;
                }
                closes[mirror] += 1;
                let peer = format!("mirror-{mirror}");
                let live = Trigger::Count(closes[mirror], Box::new(Trigger::Linked(peer.clone())));
                sync.control.push(SyncStep {
                    action: SyncAction::CloseIfLinked { peer },
                    when: Some(match when {
                        Some(when) => Trigger::All(vec![live, when]),
                        None => live,
                    }),
                });
            }
            3 => {
                let len = 1 + u32::from(a % 4);
                tip += len;
                sync.control.push(SyncStep {
                    action: SyncAction::Grow { len },
                    when,
                });
            }
            4 => {
                branch += 1;
                let at = 1 + u32::from(a) % (tip - 1);
                let len = tip - at + 1 + u32::from(a >> 6);
                tip = at + len;
                sync.control.push(SyncStep {
                    action: SyncAction::Reset {
                        fork: Fork { branch, at, len },
                    },
                    when,
                });
            }
            5 | 6 => sync.block_policy.holds.push(composed_hold(a, b)),
            _ => {
                sync.control.push(SyncStep {
                    action: SyncAction::Teardown,
                    when,
                });
                break;
            }
        }
    }
    SyncWireScenario {
        net,
        sync,
        windows: None,
        superseded_hold: None,
        memory_budget_bytes: None,
    }
}

/// A control step's trigger from its slot's `b`: none, or the first to
/// fourth (bits 0 and 1) admission, mirror range read, needed-block query
/// or apply submitted, or that many seconds into the run (bits 2 and up,
/// modulo 6).
fn composed_trigger(b: u8) -> Option<Trigger> {
    let nth = 1 + u32::from(b & 3);
    let submitted = |kind| Some(Trigger::Submitted(block_policy::Select::nth(kind, nth)));
    match (b >> 2) % 6 {
        0 => None,
        1 => Some(super::sync::admitted(nth)),
        2 => submitted(block_policy::BlockKind::MirrorRead),
        3 => submitted(block_policy::BlockKind::Query),
        4 => submitted(block_policy::BlockKind::Apply),
        _ => Some(Trigger::VirtualTime(1_000 * u64::from(nth))),
    }
}

/// A block hold from its slot's `a` and `b`: it holds mirror range reads,
/// needed-block queries, admissions or applies (`a` bits 0 and 1), each
/// one or only the first to fourth (bit 2 clear, then bits 3 and 4), each
/// for [`HOLD_UNTIL_MS`] at `b` modulo 8 after its submission, then offers
/// the operation's ordinary outcome.
fn composed_hold(a: u8, b: u8) -> block_policy::Hold {
    use block_policy::{BlockAllow, BlockKind, Select};
    let (kind, then) = match a & 3 {
        0 => (BlockKind::MirrorRead, BlockAllow::Serve),
        1 => (BlockKind::Query, BlockAllow::Answer),
        2 => (BlockKind::Admit, BlockAllow::Admit),
        _ => (BlockKind::Apply, BlockAllow::Committed),
    };
    let select = if a & 4 != 0 {
        Select {
            kind: Some(kind),
            ..Select::default()
        }
    } else {
        Select::nth(kind, 1 + u32::from((a >> 3) & 3))
    };
    block_policy::Hold {
        select,
        until: Trigger::VirtualTime(HOLD_UNTIL_MS[usize::from(b & 7)]),
        then: vec![then],
        deadline_ms: None,
    }
}

/// `sync/wire`'s node, mirrors and properties with the chain, topology,
/// links, control steps and block holds a prefix composes
/// ([`sync_composed`]), so the fuzzer chooses how many reorgs, closes,
/// stalls and growths a run takes and in what order, where `sync/wire`
/// picks one saved scenario or one combined byte.
pub struct SyncComposed(SyncWire);

impl HasView for SyncComposed {
    type View = SyncView;

    /// [`SyncWire`]'s.
    fn ended(&self, end: &crate::RunEnd) {
        self.0.ended(end);
    }

    /// [`SyncWire`]'s.
    fn end_violations(&self, log: &crate::ObservationLog) -> Vec<crate::property::EndViolation> {
        self.0.end_violations(log)
    }

    /// [`SyncWire`]'s.
    fn view(&self) -> SyncView {
        self.0.view()
    }

    /// [`SyncWire`]'s.
    fn captured(&self) -> Capture {
        self.0.captured()
    }
}

impl Subsystem for SyncComposed {
    const NAME: &'static str = "sync/composed";
    // A u8 always fits in a u32.
    const GENERATOR: u32 = SYNC_COMPOSED_VERSION as u32;
    const SCENARIO_BYTES: usize = SYNC_COMPOSED_BYTES;
    type Scenario = SyncWireScenario;
    type Prepared = SyncPorts;

    /// [`wire_config`].
    fn config(scenario: &SyncWireScenario) -> RunConfig {
        wire_config(scenario)
    }

    /// [`SyncWire`]'s.
    fn prepare(scenario: &SyncWireScenario) -> SyncPorts {
        SyncWire::prepare(scenario)
    }

    /// [`SyncWire`]'s.
    fn build(sim: &mut Sim, scenario: &SyncWireScenario, ports: SyncPorts) -> Self {
        SyncComposed(SyncWire::build(sim, scenario, ports))
    }

    /// [`wire_harness_properties`].
    fn properties() -> Vec<Property<Self>> {
        wire_harness_properties()
    }

    /// [`SyncWire`]'s.
    fn sources() -> Sources {
        SyncWire::sources()
    }

    /// One family, the grammar, whose tag is [`SYNC_COMPOSED_VERSION`].
    fn families() -> Vec<ScenarioFamily> {
        vec![ScenarioFamily::free("composed", [SYNC_COMPOSED_VERSION])]
    }

    /// Whether `prefix` starts with [`SYNC_COMPOSED_VERSION`].
    fn accepts(prefix: &[u8]) -> bool {
        prefix.first() == Some(&SYNC_COMPOSED_VERSION)
    }

    /// [`sync_composed`] of the bytes after [`SYNC_COMPOSED_VERSION`], or
    /// [`SavedWire::Honest`] for a prefix with another first byte.
    fn draw(prefix: &[u8]) -> SyncWireScenario {
        match prefix.split_first() {
            Some((&SYNC_COMPOSED_VERSION, grammar)) => sync_composed(grammar),
            _ => SavedWire::Honest.scenario(),
        }
    }
}
