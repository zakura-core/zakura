//! Header-sync harnesses. `header-sync/smoke` runs the reactor on the step
//! loop with inert storage and no peers. `header-sync/storage` runs a node
//! on controlled storage with mirror peers (M4), with the node's reactor
//! tapped. `header-sync/wire` runs the same node behind its real transport
//! on simulated QUIC, with scripted peers (M5-C), honest ones (M5-D) and
//! adversaries (M5-E1), checked by header sync's generated table properties
//! and `session/honest-never-punished` (P3d).

use std::{any::Any, borrow::Borrow, cell::RefCell, collections::BTreeSet, rc::Rc, sync::Arc};

use serde::{Deserialize, Serialize};
use zakura_chain::{block::Height, parameters::Network};
use zakura_header_chain::Frontier;
use zakura_network::zakura::{
    spawn_header_sync_reactor, v9::RegtestMessage, FullStateFrontiers, HeaderSyncStartup,
    ZakuraHeaderSyncConfig, LOCAL_MAX_MESSAGE_BYTES,
};
use zakura_node_services::header_chain::InertHeaderChainPort;
use zakura_scheduler::SchedulerConfig;

use zakura_storage::header::{
    adapter::EnginePort, controlled::ControlledPort, fixture::HeaderFixture,
};

use super::{
    evidence_hub::EvidenceHub,
    header_node::view::HeaderSyncView,
    header_node::StartupFacts,
    header_node::{
        policy, preload, tap::ReactorTap, Chain, Control, ControlStep, DispatchFlag, Effects,
        HeaderNode, Policy, PolicySource, Topology,
    },
    wire_node::{TransportTap, WireNode},
    wire_plans::SavedPlan,
    NetScenario, Subsystem,
};
use crate::{
    domain::session::{honest_never_punished, HonestPeers},
    observe::StorageFact,
    peer::{
        adversary::Act, honest::HonestChain, limit::ADVERSARY_RATE, Dialer as PeerDialer,
        PeerAction, PeerControl, PeerFailure, PeerRecords, PeerSpec, PeerTask, NODE_ADDR,
    },
    property::{
        Assume, AssumeRecord, Assumptions, Capture, CheckCtx, ComponentKind, HasView, Kind,
        Property, PropertyId, Source, Sources, Status, Verdict,
    },
    storage::HEADER_SERVICE,
    table::{downstream_properties, sender_properties, table_properties, Effect, StreamIdentity},
    Observation, RunConfig, Sim,
};

/// The smoke run's scenario. It has no knobs yet.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {}

/// The header-sync reactor at Regtest genesis, with inert storage and no
/// peers. It checks only the step loop's properties, so a run shows that the
/// reactor runs on a `Plan` and shuts down.
pub struct Smoke {
    /// The reactor's handle and action channel. They live until the run
    /// ends, so the reactor doesn't see its inputs close.
    _inputs: Box<dyn Any>,
}

impl HasView for Smoke {
    type View = ();

    /// Nothing, since no property reads it.
    fn view(&self) {}
}

impl Subsystem for Smoke {
    const NAME: &'static str = "header-sync/smoke";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Scenario;
    type Prepared = ();

    /// 2 workers, small stacks, and 2,000 live steps.
    fn config(_: &Scenario) -> RunConfig {
        RunConfig {
            scheduler: SchedulerConfig {
                workers: 2,
                // The reactor's stack needs are small, and a small stack keeps
                // the pool cheap.
                stack_size: 64 * 1024,
                ..SchedulerConfig::default()
            },
            budget: 2_000,
            drain_bound: 1_000,
            ..RunConfig::default()
        }
    }

    /// Nothing to set up.
    fn prepare(_: &Scenario) {}

    /// Spawns the reactor and a root task that joins it.
    fn build(sim: &mut Sim, _: &Scenario, (): ()) -> Self {
        let (handle, actions, reactor) =
            spawn_header_sync_reactor(startup()).expect("the Regtest genesis startup is valid");
        sim.scheduler().spawn_root("join", async move {
            let _ = reactor.await;
        });
        Smoke {
            _inputs: Box::new((handle, actions)),
        }
    }

    /// The only scenario.
    fn draw(_: &[u8]) -> Scenario {
        Scenario {}
    }
}

/// Header-sync startup on Regtest at genesis, with inert storage.
fn startup() -> HeaderSyncStartup {
    let network = Network::new_regtest(Default::default());
    let anchor = (Height(0), network.genesis_hash());
    let mut startup = HeaderSyncStartup::new(
        network,
        anchor,
        FullStateFrontiers {
            finalized_height: anchor.0,
            verified_block_tip: anchor.0,
            verified_block_hash: anchor.1,
        },
        Some(anchor),
        ZakuraHeaderSyncConfig::default(),
        LOCAL_MAX_MESSAGE_BYTES,
    );
    startup.header_chain_port = Arc::new(InertHeaderChainPort);
    startup
}

/// A run reported a direct-dispatch violation: some reactor sent a storage
/// call to its action channel, so its port never saw it.
pub const DIRECT_DISPATCH: &str = "harness/direct-dispatch";

/// The node's storage operations get rising IDs, each completes at most
/// once, and only after its submission (M4.1).
pub const OPERATION_IDS: &str = "header-sync/storage-operation-ids";

/// Which end of a mirror's link dials.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Dialer {
    /// The mirror dials the node.
    Mirror,
    /// The node dials the mirror.
    Node,
}

/// A mirror peer: real header-sync code on an engine port. It's named
/// `mirror-<i>` by its index in [`StorageScenario::mirrors`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MirrorSpec {
    /// The chain it holds when the run starts.
    pub chain: Chain,
    /// Which end of its link to the node dials.
    pub dialer: Dialer,
}

/// The `header-sync/storage` scenario.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageScenario {
    /// The chain the node under test holds when the run starts.
    pub node: Chain,
    /// The mirror peers, each linked once to the node at setup.
    pub mirrors: Vec<MirrorSpec>,
    /// How the run completes the node's storage operations.
    pub policy: Policy,
    /// Link actions the `Plan` may take, in order.
    #[serde(default)]
    pub controls: Vec<ControlStep>,
}

impl Default for StorageScenario {
    /// The node at genesis and `mirror-0` at A5, under the converging policy.
    fn default() -> Self {
        StorageScenario {
            node: Chain::default(),
            mirrors: vec![MirrorSpec {
                chain: Chain::main(5),
                dialer: Dialer::Mirror,
            }],
            policy: Policy::converging(),
            controls: Vec::new(),
        }
    }
}

/// The scenario's storage, with each actor's chain committed before the
/// run. Cloning shares the ports, so a test can read them after a run.
#[derive(Clone)]
pub struct Ports {
    /// The fixture every chain comes from.
    pub fixture: HeaderFixture,
    /// The node's controlled port.
    pub node: Arc<ControlledPort>,
    /// Each mirror's engine port, in scenario order.
    pub mirrors: Vec<Arc<EnginePort>>,
}

/// The node under test, named `node`, on a controlled port with mirror
/// peers, checked by [`storage_properties`].
pub struct Storage {
    port: Arc<ControlledPort>,
    dispatch: DispatchFlag,
    owners: Rc<RefCell<HeaderSyncView>>,
    /// The nodes and links. They live until the run ends, so no reactor sees
    /// its inputs close.
    _topology: Rc<Topology>,
}

/// What [`storage_properties`] read between steps.
pub struct StorageView {
    /// The node's port, for its pending operations and its engine's state.
    pub port: Arc<ControlledPort>,
    /// Whether any reactor dispatched a storage call externally.
    pub direct_dispatch_broken: bool,
    /// The peers the scenario marks `HonestPeer`, by index. Only
    /// `header-sync/wire` has peers to mark.
    pub honest_peers: Rc<BTreeSet<usize>>,
}

impl HonestPeers for StorageView {
    /// True if `index` is in [`StorageView::honest_peers`].
    fn is_honest(&self, index: usize) -> bool {
        self.honest_peers.contains(&index)
    }
}

/// What both header tiers' checks read between steps: the storage view,
/// and the owners' components as the evidence hub folded them (M6a-2).
pub struct HeaderWireView {
    /// The node's port and the dispatch guard's flag.
    pub storage: StorageView,
    /// The tier's components, as of the last boundary the hub closed.
    pub owners: Rc<RefCell<HeaderSyncView>>,
}

impl Borrow<StorageView> for HeaderWireView {
    /// The storage view.
    fn borrow(&self) -> &StorageView {
        &self.storage
    }
}

impl HonestPeers for HeaderWireView {
    /// The storage view's answer.
    fn is_honest(&self, index: usize) -> bool {
        self.storage.is_honest(index)
    }
}

/// The composite view of a header tier's `port`, `dispatch` flag, `honest`
/// peers and `owners`.
fn header_view(
    port: &Arc<ControlledPort>,
    dispatch: &DispatchFlag,
    honest: &Rc<BTreeSet<usize>>,
    owners: &Rc<RefCell<HeaderSyncView>>,
) -> HeaderWireView {
    HeaderWireView {
        storage: StorageView {
            port: port.clone(),
            direct_dispatch_broken: dispatch.get(),
            honest_peers: honest.clone(),
        },
        owners: owners.clone(),
    }
}

impl HasView for Storage {
    type View = HeaderWireView;

    /// The node's port, the dispatch guard's flag and the owners. Only
    /// `header-sync/wire` has peers to mark honest.
    fn view(&self) -> HeaderWireView {
        header_view(&self.port, &self.dispatch, &Rc::default(), &self.owners)
    }

    /// The boundary the hub last closed.
    fn captured(&self) -> Capture {
        RefCell::borrow(&self.owners).captured()
    }
}

impl Subsystem for Storage {
    const NAME: &'static str = "header-sync/storage";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = StorageScenario;
    type Prepared = Ports;

    /// [`storage_config`].
    fn config(_: &StorageScenario) -> RunConfig {
        storage_config()
    }

    /// Commits each actor's chain to its port.
    ///
    /// # Panics
    ///
    /// If a hold or control step waits for a trigger the node never records
    /// ([`policy::undriven`]).
    fn prepare(scenario: &StorageScenario) -> Ports {
        let triggers = scenario.policy.holds.iter().map(|hold| &hold.until).chain(
            scenario
                .controls
                .iter()
                .filter_map(|step| step.when.as_ref()),
        );
        if let Some(why) = triggers.filter_map(policy::undriven).next() {
            panic!("{why}");
        }
        let fixture = HeaderFixture::regtest();
        let node = Arc::new(ControlledPort::new(&fixture));
        preload(node.engine(), &fixture, &scenario.node);
        let mirrors = scenario
            .mirrors
            .iter()
            .map(|mirror| {
                let port = Arc::new(EnginePort::new(&fixture));
                preload(&port, &fixture, &mirror.chain);
                port
            })
            .collect();
        Ports {
            fixture,
            node,
            mirrors,
        }
    }

    /// Spawns the node on its controlled port under the scenario's policy,
    /// with its reactor tapped, then each mirror, links each mirror to the
    /// node once, and adds the `Control` source. Only the node is tapped.
    fn build(sim: &mut Sim, scenario: &StorageScenario, ports: Ports) -> Self {
        let dispatch = DispatchFlag::default();
        let effects = Effects::default();
        let mut hub = EvidenceHub::new();
        let owners = Rc::new(RefCell::new(HeaderSyncView::new(SourceManifest::storage())));
        hub.owners(owners.clone());
        let storage = PolicySource::new(
            sim,
            ports.node.clone(),
            scenario.policy.clone(),
            effects.clone(),
        );
        let (observer, tap) = ReactorTap::new(sim, &ports.node, "node");
        hub.reactor(tap.link(), Some(owners.clone()));
        let storage = storage.with_tap(tap).behind(&mut hub);
        let predicates = storage.predicates();
        sim.add_source(Box::new(storage));
        let mut nodes = vec![HeaderNode::spawn(
            sim,
            "node",
            1,
            ports.node.clone(),
            &ports.fixture,
            scenario.node.tip(&ports.fixture),
            dispatch.clone(),
            Some(observer),
        )];
        for (index, ((spec, port), seed)) in scenario
            .mirrors
            .iter()
            .zip(&ports.mirrors)
            .zip(2..)
            .enumerate()
        {
            nodes.push(HeaderNode::spawn(
                sim,
                &format!("mirror-{index}"),
                seed,
                port.clone(),
                &ports.fixture,
                spec.chain.tip(&ports.fixture),
                dispatch.clone(),
                None,
            ));
        }
        let topology = Rc::new(Topology::new(nodes, effects));
        for (index, spec) in (1..).zip(&scenario.mirrors) {
            match spec.dialer {
                Dialer::Mirror => topology.link(index, 0),
                Dialer::Node => topology.link(0, index),
            };
        }
        let control = Control::new(
            sim,
            topology.clone(),
            ports.fixture.clone(),
            scenario.controls.clone(),
            predicates,
        );
        sim.add_source(Box::new(control));
        // Last, so it writes everything the others observed.
        sim.add_source(Box::new(hub));
        Storage {
            port: ports.node,
            dispatch,
            owners,
            _topology: topology,
        }
    }

    /// [`storage_properties`].
    fn properties() -> Vec<Property<Self>> {
        storage_properties()
    }

    /// [`storage_sources`].
    fn sources() -> Sources {
        storage_sources()
    }

    /// The default scenario.
    fn draw(_: &[u8]) -> StorageScenario {
        StorageScenario::default()
    }
}

/// The `header-sync/wire` scenario.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireScenario {
    /// The network: its seed, links and loss gate.
    #[serde(default)]
    pub net: NetScenario,
    /// The chain the node holds when the run starts.
    pub node: Chain,
    /// The scripted peers, named `peer-<i>` by index.
    pub peers: Vec<PeerSpec>,
    /// How the run completes the node's storage operations.
    pub policy: Policy,
}

impl WireScenario {
    /// The peers the scenario marks `HonestPeer`, by index: each that
    /// [`PeerSpec::is_honest`].
    pub fn honest_peers(&self) -> BTreeSet<usize> {
        (0..)
            .zip(&self.peers)
            .filter(|(_, peer)| peer.is_honest())
            .map(|(index, _)| index)
            .collect()
    }
}

impl Default for WireScenario {
    /// The node at genesis, dialed by one peer that offers header sync
    /// alone, under the converging policy.
    fn default() -> Self {
        WireScenario {
            net: NetScenario::default(),
            node: Chain::default(),
            peers: vec![PeerSpec::dialing()],
            policy: Policy::converging(),
        }
    }
}

/// The wire scenario's storage: the fixture, the node's controlled port
/// with its chain committed, and each honest peer's chain. Cloning shares
/// them, so a test can read them after a run.
#[derive(Clone)]
pub struct WirePorts {
    /// The fixture the node's chain comes from.
    pub fixture: HeaderFixture,
    /// The node's controlled port.
    pub node: Arc<ControlledPort>,
    /// Each peer's chain, by index, if it runs the honest script.
    pub chains: Vec<Option<Arc<HonestChain>>>,
    /// What the node's reactor was started with, once the run builds it.
    pub startup: Rc<RefCell<Option<StartupFacts>>>,
    /// The peers' records the identity map hasn't placed yet.
    records: PeerRecords,
}

impl WirePorts {
    /// The node's selected header tip, read from its engine: the end-of-run
    /// check that sync converged, as M4's tests read it.
    pub fn selected_tip(&self) -> Frontier {
        self.node.engine().snapshot().frontiers.header_best
    }

    /// How many peer records are still waiting for their stream's identity
    /// rows. A run ends with some only if a peer wrote on a stream the node
    /// hasn't admitted yet, so a test whose streams all reach admission
    /// requires none.
    pub fn unplaced_records(&self) -> usize {
        self.records.unplaced()
    }
}

/// The node under test, named `node`, on a controlled port behind its real
/// transport on simulated QUIC, with scripted peers. It's checked by
/// [`wire_properties`].
pub struct HeaderWire {
    port: Arc<ControlledPort>,
    dispatch: DispatchFlag,
    owners: Rc<RefCell<HeaderSyncView>>,
    /// The peers the scenario marks `HonestPeer`.
    honest: Rc<BTreeSet<usize>>,
    /// The node and the peers' state. They live until the run ends, so no
    /// task sees its inputs close.
    _keep: Box<dyn Any>,
}

impl HasView for HeaderWire {
    type View = HeaderWireView;

    /// The node's port, the dispatch guard's flag, the honest peers and the
    /// owners.
    fn view(&self) -> HeaderWireView {
        header_view(&self.port, &self.dispatch, &self.honest, &self.owners)
    }

    /// The boundary the hub last closed.
    fn captured(&self) -> Capture {
        RefCell::borrow(&self.owners).captured()
    }
}

impl Subsystem for HeaderWire {
    const NAME: &'static str = "header-sync/wire";
    const GENERATOR: u32 = 2;
    const SCENARIO_BYTES: usize = 2;
    type Scenario = WireScenario;
    type Prepared = WirePorts;

    /// [`wire_config`], with twice its budget for a scenario with status
    /// bursts: emptying a `Status` bucket takes about 600 frames, and the
    /// node about 40 steps to read each.
    fn config(scenario: &WireScenario) -> RunConfig {
        let burst = scenario.peers.iter().any(|peer| {
            peer.script.contains(&PeerAction::Act {
                act: Act::StatusBurst,
            })
        });
        let config = wire_config();
        RunConfig {
            budget: if burst {
                2 * config.budget
            } else {
                config.budget
            },
            ..config
        }
    }

    /// Commits the node's chain to its port, and builds each honest peer's
    /// chain.
    fn prepare(scenario: &WireScenario) -> WirePorts {
        let fixture = HeaderFixture::regtest();
        let node = Arc::new(ControlledPort::new(&fixture));
        preload(node.engine(), &fixture, &scenario.node);
        let chains = scenario
            .peers
            .iter()
            .map(|peer| {
                assert!(
                    peer.adversary.is_none() || peer.honest.is_some(),
                    "an adversary needs an honest chain"
                );
                peer.honest.as_ref().map(|honest| {
                    let start = honest.subscribe.as_ref().map_or(0, |spec| spec.start);
                    assert!(
                        start <= honest.chain,
                        "an honest peer subscribes from a height on its own chain"
                    );
                    Arc::new(HonestChain::new(&fixture, honest.chain))
                })
            })
            .collect();
        WirePorts {
            fixture,
            node,
            chains,
            startup: Rc::default(),
            records: PeerRecords::default(),
        }
    }

    /// Adds the network, then spawns the node as `header-sync/storage` does,
    /// with its reactor tapped, behind its transport, which its own tap
    /// reports. Then spawns each peer, and a dial for each peer the node
    /// dials, and adds the peers' action source. The evidence hub, added
    /// last, writes what every source observed.
    fn build(sim: &mut Sim, scenario: &WireScenario, ports: WirePorts) -> Self {
        let mut hub = EvidenceHub::new();
        let owners = Rc::new(RefCell::new(HeaderSyncView::new(SourceManifest::wire())));
        hub.owners(owners.clone());
        let net = scenario.net.net();
        net.write_to(hub.out());
        sim.add_source(Box::new(net.clone()));
        let dispatch = DispatchFlag::default();
        let storage = PolicySource::new(
            sim,
            ports.node.clone(),
            scenario.policy.clone(),
            Effects::default(),
        );
        let (observer, tap) = ReactorTap::new(sim, &ports.node, "node");
        hub.reactor(tap.link(), Some(owners.clone()));
        sim.add_source(Box::new(storage.with_tap(tap).behind(&mut hub)));
        let header = HeaderNode::spawn(
            sim,
            "node",
            1,
            ports.node.clone(),
            &ports.fixture,
            scenario.node.tip(&ports.fixture),
            dispatch.clone(),
            Some(observer),
        );
        *ports.startup.borrow_mut() = Some(header.startup.clone());
        let names: Vec<String> = (0..scenario.peers.len())
            .map(|index| format!("peer-{index}"))
            .collect();
        let (observer, tap) = TransportTap::new(sim, "node", net.clone(), names.clone());
        let dials = tap.dials();
        sim.add_source(Box::new(tap.writing_to(hub.out())));
        let node = WireNode::spawn(
            sim,
            header,
            ports.fixture.network(),
            net.sim(),
            scenario.net.seed,
            observer,
            dials,
        );
        let failure = PeerFailure::default();
        let mut control = PeerControl::new(failure.clone());
        hub.place_peers(ports.records.clone());
        for ((index, spec), name) in scenario.peers.iter().enumerate().zip(names) {
            let (readiness, commands) =
                control.add(&name, spec.script.clone(), spec.adversary.is_some());
            let (listening, listened) = zakura_rt::sync::watch::channel(false);
            let task = PeerTask {
                name,
                index,
                spec: spec.clone(),
                seed: scenario.net.seed,
                net: net.sim(),
                node: zakura_quic::NodeAddr::with_addrs(
                    zakura_quic::sim::SeededInputs::node_key(scenario.net.seed, 0).public(),
                    [NODE_ADDR],
                ),
                commands,
                readiness,
                failure: failure.clone(),
                listening,
                chain: ports.chains[index].clone(),
                records: ports.records.clone(),
                taken: 0,
            };
            sim.scheduler().spawn_root("peer", task.run());
            if spec.dialer == PeerDialer::Node {
                node.dial(sim, index, listened);
            }
        }
        sim.add_source(Box::new(control));
        sim.add_source(Box::new(hub));
        HeaderWire {
            port: ports.node,
            dispatch,
            owners,
            honest: Rc::new(scenario.honest_peers()),
            _keep: Box::new(node),
        }
    }

    /// [`wire_properties`].
    fn properties() -> Vec<Property<Self>> {
        wire_properties()
    }

    /// The storage harness's sources, plus the network's datagrams, the
    /// peers' frames, the node's closes, and the wire tier's components.
    fn sources() -> Sources {
        SourceManifest::wire().sources(Sources::observed(&[
            Source::Schedule,
            Source::Storage,
            Source::View,
            Source::Datagrams,
            Source::Frames,
            Source::Closes,
        ]))
    }

    /// For a scenario with an adversary, `BoundedAdversary` at
    /// [`ADVERSARY_RATE`] actions per virtual second, the budget
    /// [`PeerControl`] holds every adversary to (M5-Q9). A scenario of
    /// honest and scripted peers has no hostile peer to bound. For a
    /// scenario with honest peers, `HonestPeer` with their names
    /// ([`WireScenario::honest_peers`]).
    fn assumptions(scenario: &WireScenario) -> Assumptions<Self> {
        let mut declared = Vec::new();
        if scenario.peers.iter().any(|peer| peer.adversary.is_some()) {
            declared.push(AssumeRecord {
                assume: Assume::BoundedAdversary,
                params: serde_json::json!({ "actions_per_second": ADVERSARY_RATE }),
            });
        }
        let honest = scenario.honest_peers();
        if !honest.is_empty() {
            let names: Vec<String> = honest.iter().map(|index| format!("peer-{index}")).collect();
            declared.push(AssumeRecord {
                assume: Assume::HonestPeer,
                params: serde_json::json!({ "peers": names }),
            });
        }
        Assumptions::declare(declared)
    }

    /// The saved Plan's scenario `prefix` tags ([`SavedPlan::tag`]), or
    /// the default scenario.
    fn draw(prefix: &[u8]) -> WireScenario {
        SavedPlan::from_tag(prefix).map_or_else(WireScenario::default, SavedPlan::scenario)
    }
}

/// The wire run's config: the default scheduler, and room for QUIC
/// handshakes, stream setup and a few scripted actions.
pub fn wire_config() -> RunConfig {
    RunConfig {
        scheduler: SchedulerConfig::default(),
        budget: 30_000,
        drain_bound: 2_000,
        ..RunConfig::default()
    }
}

/// The storage run's config: 2 workers, the default stack, which engine
/// planning needs, and room to sync and then idle on timers.
pub fn storage_config() -> RunConfig {
    RunConfig {
        scheduler: SchedulerConfig {
            workers: 2,
            ..SchedulerConfig::default()
        },
        budget: 4_000,
        drain_bound: 1_000,
        ..RunConfig::default()
    }
}

/// The family header sync's generated property IDs name.
pub const FAMILY: &str = "header-sync";

/// `header-sync/wire`'s properties: [`storage_properties`], then header
/// sync's generated receiver and sender properties (M5-Q13), then
/// `session/honest-never-punished`. Header sync's adapter observes none of
/// the downstream templates' effects, so it registers none of them.
pub fn wire_properties<S: HasView>() -> Vec<Property<S>>
where
    S::View: Borrow<StorageView> + HonestPeers,
{
    let mut properties = storage_properties();
    properties.extend(table_properties(FAMILY, &RegtestMessage::LAYOUT));
    properties.extend(sender_properties(FAMILY, &RegtestMessage::LAYOUT));
    properties.push(honest_never_punished());
    properties
}

/// What [`staged_work_properties`] read: staged work's effect rows, first,
/// and what the downstream work template reads.
const STAGED_WORK_NEEDS: &[Source] = &[
    Source::Component(ComponentKind::StagedWork),
    Source::Frames,
    Source::Closes,
    Source::Storage,
];

/// The header family's `receiver-work-authorized` properties, one per
/// response row, each needing staged work's effect rows (M6a-5a), so
/// `header-sync/storage` reports them `NotApplicable` with that reason.
/// Neither header harness registers them yet. P3d does, with the
/// peer-side credit rows `receiver-credit-conserved` needs.
pub fn staged_work_properties<S: HasView>() -> Vec<Property<S>> {
    downstream_properties(
        FAMILY,
        &RegtestMessage::LAYOUT,
        &[Effect::Work],
        StreamIdentity::Exact,
    )
    .into_iter()
    .filter(|property| property.id.as_str().ends_with("/receiver-work-authorized"))
    .map(|property| Property {
        needs: STAGED_WORK_NEEDS,
        ..property
    })
    .collect()
}

/// The storage harness's properties, [`OPERATION_IDS`] then
/// [`DIRECT_DISPATCH`], both active. A test harness that wraps
/// [`Storage`] with its view registers them too.
pub fn storage_properties<S: HasView>() -> Vec<Property<S>>
where
    S::View: Borrow<StorageView>,
{
    vec![
        Property {
            id: PropertyId::new(OPERATION_IDS),
            claim: "the node's storage operations get rising IDs, and each completes at most \
                    once, after its submission",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::Storage],
            status: Status::Active,
            check: check_operation_ids::<S>,
            witness: None,
        },
        Property {
            id: PropertyId::new(DIRECT_DISPATCH),
            claim: "every storage call goes to the node's port",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::View],
            status: Status::Active,
            check: check_direct_dispatch::<S>,
            witness: None,
        },
    ]
}

/// What [`storage_properties`] read: the node's storage facts and the
/// harness's view, plus the step loop's task observations, with the
/// storage tier's components. It has no peers and no transport, so it
/// excludes their frames and closes.
pub fn storage_sources() -> Sources {
    SourceManifest::storage().sources(
        Sources::observed(&[Source::Schedule, Source::Storage, Source::View])
            .excluding(Source::Frames, "no scripted peers")
            .excluding(Source::Closes, "no transport"),
    )
}

/// What shows that a component exists, because it acted (M6a, rule 7).
/// Before it, the component is `Initializing`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Evidence {
    /// The harness built it.
    HarnessBuilt,
    /// The reactor's first `Resumed{arm: Start}`, which binds its task.
    ReactorStarted,
    /// The reactor's first `Settled`.
    ReactorSettled,
    /// The first `EntriesTaken`.
    EntriesTaken,
    /// A session's `Installed` record and its `RegistryFanout`.
    Installed,
    /// The first output's `Allocated`.
    Allocated,
    /// The first transport `Registered`.
    Registered,
    /// The first header stream's `Admitted`.
    HeaderAdmitted,
    /// The connection's first `ConnResumed{turn: 0}`.
    ConnStarted,
    /// The connection's first `ConnResumed` or stream event.
    ConnActive,
    /// The gauge's first set.
    GaugeSet,
}

/// When a component ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndRule {
    /// Its read reports `Gone`.
    Gone,
    /// Its loop's final `Settled`.
    FinalSettled,
    /// With the component that holds its registry.
    With(ComponentKind),
    /// At each output's terminal record, or pending at the freeze.
    Terminal,
    /// At `ConnEnded`, with no live task or permit.
    ConnEnded,
    /// When its last tag drops.
    LastTag,
    /// When the run ends.
    RunEnd,
}

/// One component a tier has: the evidence that it exists, its end rule,
/// and the parts a reconciliation over it names, itself included. Empty
/// `parts` means it has no reconciliation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ManifestRow {
    /// The component.
    pub kind: ComponentKind,
    /// What shows it exists.
    pub exists: Evidence,
    /// When it ends.
    pub ends: EndRule,
    /// The parts its reconciliation names.
    pub parts: &'static [ComponentKind],
}

/// A header tier's components (M6a, Components and Tiers): the rows it
/// has, and the ones it excludes, each with its reason. A property that
/// needs an excluded component is `NotApplicable` with that reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceManifest {
    rows: Vec<ManifestRow>,
    /// Effect evidence it observes, which has no read and so no row.
    effects: Vec<ComponentKind>,
    excluded: Vec<(ComponentKind, &'static str)>,
}

/// A manifest row.
const fn row(
    kind: ComponentKind,
    exists: Evidence,
    ends: EndRule,
    parts: &'static [ComponentKind],
) -> ManifestRow {
    ManifestRow {
        kind,
        exists,
        ends,
        parts,
    }
}

/// The rows both header tiers have. Budget reconciliation names the
/// reactor, since every budget write runs on its task (rule 5).
fn node_rows() -> [ManifestRow; 5] {
    use ComponentKind as K;
    use EndRule as R;
    use Evidence as E;
    const BUDGET: &[ComponentKind] = &[K::Budget, K::Reactor, K::CallHolders, K::Transfers];
    [
        row(K::Reactor, E::ReactorStarted, R::FinalSettled, &[]),
        row(K::Budget, E::ReactorSettled, R::Gone, BUDGET),
        row(K::CallHolders, E::ReactorSettled, R::With(K::Budget), &[]),
        row(K::Transfers, E::EntriesTaken, R::With(K::Reactor), &[]),
        row(K::SelectedChain, E::HarnessBuilt, R::RunEnd, &[]),
    ]
}

/// The rows only `header-sync/wire` has: its transport's, then its gauges.
/// A connection's permits reconcile with its loop and session tasks.
fn transport_rows() -> [ManifestRow; 8] {
    use ComponentKind as K;
    use EndRule as R;
    use Evidence as E;
    const PERMITS: &[ComponentKind] = &[K::StreamPermits, K::Connection, K::SessionTasks];
    [
        row(K::Publisher, E::Installed, R::Gone, &[]),
        row(K::Output, E::Allocated, R::Terminal, &[]),
        row(K::SupervisorMap, E::Registered, R::Gone, &[]),
        row(K::HeaderSessions, E::HeaderAdmitted, R::Gone, &[]),
        row(K::Connection, E::ConnStarted, R::ConnEnded, &[]),
        row(K::SessionTasks, E::ConnActive, R::LastTag, &[]),
        row(K::StreamPermits, E::ConnActive, R::LastTag, PERMITS),
        row(K::Gauges, E::GaugeSet, R::RunEnd, &[]),
    ]
}

/// Why neither header tier has session slots.
const NO_SESSIONS: &str = "header sync reserves no sessions";

impl SourceManifest {
    /// `header-sync/wire`'s components: all but the session slots, and its
    /// staged work's effect rows.
    pub fn wire() -> Self {
        SourceManifest {
            rows: node_rows().into_iter().chain(transport_rows()).collect(),
            effects: vec![ComponentKind::StagedWork],
            excluded: vec![(ComponentKind::SessionSlots, NO_SESSIONS)],
        }
    }

    /// `header-sync/storage`'s components: the node's. It has no transport
    /// and no session slots, and it excludes gauges, since its mirror
    /// reactors share the node's gauge names. It excludes staged work's
    /// effect rows too, since its in-memory links have no read seqs to name
    /// a frame by.
    pub fn storage() -> Self {
        let mut excluded: Vec<_> = transport_rows()
            .iter()
            .filter(|row| row.kind != ComponentKind::Gauges)
            .map(|row| (row.kind, "no transport"))
            .collect();
        excluded.push((ComponentKind::SessionSlots, NO_SESSIONS));
        excluded.push((
            ComponentKind::Gauges,
            "mirror reactors share the gauge names",
        ));
        excluded.push((
            ComponentKind::StagedWork,
            "in-memory links have no read seqs",
        ));
        SourceManifest {
            rows: node_rows().to_vec(),
            effects: Vec::new(),
            excluded,
        }
    }

    /// Its rows, in order.
    pub fn rows(&self) -> &[ManifestRow] {
        &self.rows
    }

    /// `sources`, observing each row's component and each effect, and
    /// excluding the others with their reasons.
    pub fn sources(&self, sources: Sources) -> Sources {
        let observed = self
            .rows
            .iter()
            .map(|row| row.kind)
            .chain(self.effects.iter().copied());
        let sources = observed.fold(sources, |sources, kind| {
            sources.observing(Source::Component(kind))
        });
        self.excluded
            .iter()
            .fold(sources, |sources, (kind, reason)| {
                sources.excluding(Source::Component(*kind), reason)
            })
    }
}

/// A submission or completion on the node's `service` storage, instance 0.
fn operation_fact<'o>(service: &str, obs: &'o Observation) -> Option<&'o StorageFact> {
    match obs {
        Observation::Storage {
            service: logged,
            instance: 0,
            fact:
                fact @ (StorageFact::Submitted { .. }
                | StorageFact::Completed { .. }
                | StorageFact::BlockSubmitted { .. }
                | StorageFact::BlockCompleted { .. }),
            ..
        } if logged == service => Some(fact),
        _ => None,
    }
}

/// Checks [`OPERATION_IDS`] on the node's header storage.
fn check_operation_ids<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    check_service_operation_ids(HEADER_SERVICE, cx)
}

/// Checks that the node's `service` storage gives its operations rising
/// IDs and completes each at most once, after its submission, at each step
/// that logs a submission or completion there. A check keeps no state
/// between steps, so it replays the node's facts from the run's start, and
/// reports the first finding among this step's.
pub(crate) fn check_service_operation_ids<S: HasView>(
    service: &str,
    cx: &mut CheckCtx<'_, S>,
) -> Verdict {
    if !cx
        .observations()
        .any(|obs| operation_fact(service, obs).is_some())
    {
        return Verdict::NotApplicable;
    }
    let mut ids = OperationIds::default();
    for entry in cx.log.entries() {
        let Some(fact) = operation_fact(service, &entry.obs) else {
            continue;
        };
        if let Some((expected, actual)) = ids.check(fact) {
            if entry.step == cx.step {
                return cx.violated(expected, actual);
            }
        }
    }
    Verdict::Holds
}

/// Checks [`DIRECT_DISPATCH`]: violated once the dispatch guard's flag is
/// set.
fn check_direct_dispatch<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S::View: Borrow<StorageView>,
{
    if cx.view().borrow().direct_dispatch_broken {
        cx.violated(
            "every storage call goes to the node's port",
            "a reactor sent a call to its action channel",
        )
    } else {
        Verdict::Holds
    }
}

/// The node's operation IDs so far, which [`check_operation_ids`] replays
/// its facts into.
#[derive(Debug, Default)]
struct OperationIds {
    /// The last submitted ID.
    submitted: u64,
    /// Every completed ID.
    completed: BTreeSet<u64>,
}

impl OperationIds {
    /// Records `fact`, and returns what's wrong with it, if anything, as
    /// (expected, actual).
    fn check(&mut self, fact: &StorageFact) -> Option<(String, String)> {
        match *fact {
            StorageFact::Submitted { id, .. } | StorageFact::BlockSubmitted { id, .. } => {
                let expected = self.submitted + 1;
                self.submitted = id;
                (id != expected).then(|| {
                    (
                        format!("operation {expected} submitted next"),
                        format!("operation {id} submitted"),
                    )
                })
            }
            StorageFact::Completed { id, .. } | StorageFact::BlockCompleted { id, .. } => {
                if id == 0 || id > self.submitted {
                    let actual = if id == 0 {
                        "operation 0 completed, an ID no operation gets".to_owned()
                    } else {
                        format!("operation {id} completed before its submission")
                    };
                    Some((
                        format!(
                            "a completion of a submitted operation, at most {}",
                            self.submitted
                        ),
                        actual,
                    ))
                } else if !self.completed.insert(id) {
                    Some((
                        format!("operation {id} completes once"),
                        format!("operation {id} completed again"),
                    ))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use super::*;
    use crate::{
        check::Checker,
        end::{IdleStatus, Outcome, RunEnd},
        observe::{StorageRequest, StorageResult},
        property::{Registry, RegistryChecker},
        HarnessFailure, ObservationLog, PropertyOutcome,
    };

    /// A submission of operation `id`.
    fn submitted(id: u64) -> StorageFact {
        StorageFact::Submitted {
            id,
            request: StorageRequest::ContinuationLocator,
            digest: String::new(),
            call: None,
        }
    }

    /// A completion of operation `id`.
    fn completed(id: u64) -> StorageFact {
        StorageFact::Completed {
            id,
            outcome: "engine".to_owned(),
            result: StorageResult::NoLocator,
            delivered: true,
            generation: 0,
            state: String::new(),
        }
    }

    /// `check` on `facts` in order, with each fact's finding.
    fn check_all(facts: &[StorageFact]) -> Vec<Option<(String, String)>> {
        let mut ids = OperationIds::default();
        facts.iter().map(|fact| ids.check(fact)).collect()
    }

    /// The finding `check` reports.
    fn finding(expected: &str, actual: &str) -> Option<(String, String)> {
        Some((expected.to_owned(), actual.to_owned()))
    }

    /// No M4 run trips [`OPERATION_IDS`], so each of its branches gets a
    /// hand-built fact sequence: a skipped submission, a completion before
    /// its submission, and a second completion. In-order facts pass.
    #[test]
    fn check_reports_each_operation_id_violation() {
        assert_eq!(
            check_all(&[submitted(1), completed(1), submitted(2), completed(2)]),
            [None, None, None, None]
        );
        assert_eq!(
            check_all(&[submitted(1), submitted(3)]),
            [
                None,
                finding("operation 2 submitted next", "operation 3 submitted")
            ]
        );
        assert_eq!(
            check_all(&[submitted(1), completed(2)]),
            [
                None,
                finding(
                    "a completion of a submitted operation, at most 1",
                    "operation 2 completed before its submission"
                )
            ]
        );
        assert_eq!(
            check_all(&[submitted(1), completed(1), completed(1)]),
            [
                None,
                None,
                finding("operation 1 completes once", "operation 1 completed again")
            ]
        );
    }

    /// A harness with nothing to view.
    struct Nothing;

    impl HasView for Nothing {
        type View = ();

        fn view(&self) {}
    }

    /// Never runs: its component is excluded or missing.
    fn check_publisher(_: &mut CheckCtx<'_, Nothing>) -> Verdict {
        Verdict::Holds
    }

    /// A checker for one property that needs the publisher, on a harness
    /// with `sources`.
    fn publisher_checker(sources: &Sources) -> RegistryChecker<Nothing> {
        let property = Property {
            id: PropertyId::new("test/publisher"),
            claim: "a test claim on the publisher",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::Component(ComponentKind::Publisher)],
            status: Status::Active,
            check: check_publisher,
            witness: None,
        };
        RegistryChecker::new(
            Registry::new(vec![property]).expect("one property"),
            Rc::new(Nothing),
            Assumptions::none(),
            sources,
        )
    }

    /// A property that needs the publisher on `header-sync/storage`, which
    /// has no transport, is reported `NotApplicable` with that reason.
    #[test]
    fn an_excluded_component_reports_not_applicable_with_its_reason() {
        let mut checker = publisher_checker(&storage_sources());
        assert_eq!(checker.harness_failure(), None);
        let end = RunEnd {
            outcome: Outcome::Idle {
                status: IdleStatus::Quiescent,
            },
            steps: 0,
            violation: None,
            drain: None,
            harness: None,
        };
        let reports = checker.at_end(&end, &ObservationLog::default());
        let [report] = reports.as_slice() else {
            panic!("one property, one report: {reports:?}");
        };
        assert_eq!(report.property, "test/publisher");
        assert_eq!(report.outcome, PropertyOutcome::NotApplicable);
        assert_eq!(
            report.excluded.as_deref(),
            Some("Component(Publisher): no transport")
        );
    }

    /// Staged work's effect rows are a source `header-sync/wire` observes
    /// and `header-sync/storage` excludes, since its in-memory links have
    /// no read seqs, so the header family's `receiver-work-authorized`
    /// properties run on the wire tier and are `NotApplicable` on storage
    /// with that reason (M6a-5a).
    #[test]
    fn storage_excludes_staged_work() {
        let property = |sources: &Sources| {
            RegistryChecker::new(
                Registry::new(staged_work_properties()).expect("one per response row"),
                Rc::new(Nothing),
                Assumptions::none(),
                sources,
            )
        };
        assert_eq!(property(&HeaderWire::sources()).harness_failure(), None);
        let mut storage = property(&storage_sources());
        assert_eq!(storage.harness_failure(), None);
        let end = RunEnd {
            outcome: Outcome::Idle {
                status: IdleStatus::Quiescent,
            },
            steps: 0,
            violation: None,
            drain: None,
            harness: None,
        };
        let reports = storage.at_end(&end, &ObservationLog::default());
        assert_eq!(reports.len(), 2, "the page and outcome rows: {reports:?}");
        for report in reports {
            assert!(report.property.ends_with("/receiver-work-authorized"));
            assert_eq!(report.outcome, PropertyOutcome::NotApplicable);
            assert_eq!(
                report.excluded.as_deref(),
                Some("Component(StagedWork): in-memory links have no read seqs")
            );
        }
    }

    /// The same property on a harness that neither observes nor excludes
    /// the publisher fails the run.
    #[test]
    fn a_missing_component_fails_the_run() {
        assert_eq!(
            publisher_checker(&HeaderWire::sources()).harness_failure(),
            None,
            "the wire tier observes the publisher"
        );
        let sources = Sources::observed(&[Source::Schedule, Source::Storage, Source::View]);
        assert_eq!(
            publisher_checker(&sources).harness_failure(),
            Some(HarnessFailure::MissingSource {
                property: "test/publisher".to_owned(),
                source: "Component(Publisher)".to_owned(),
            })
        );
    }
}
