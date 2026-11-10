//! `sync/storage` (xsync-1): header sync and block sync together on one
//! node, over one engine, with mirror peers that run both services.
//!
//! The node under test runs its real header-sync reactor on a
//! [`ControlledPort`] and its real block-sync reactor, routines and
//! sequencer on a [`ControlledBlockService`]. Both wrap the same
//! [`EnginePort`], as zakurad's driver shares zakura-state between the two
//! services. The node starts at genesis. Its headers come only from header
//! sync, and its body work comes only from the committed views the engine
//! publishes once header sync applies them, so no test injects pending
//! work. Each service's storage operations complete under its own policy:
//! the header side's [`header_node::PolicySource`] and the block side's
//! [`block_node::PolicySource`].
//!
//! Each mirror runs header sync and block sync on one plain engine that
//! holds the whole fixture chain, headers and bodies. A link is one
//! connection: header sync's stream 5 and block sync's stream 6, each over
//! a pair of in-memory channels, with one connection token per side that
//! both services' sessions share, as a transport's connection does.
//!
//! [`SyncAction::Reset`] and [`SyncAction::Grow`] change the mirrors' chains
//! only. The node learns of the change from its mirrors' header sync, and
//! its block sync from the engine's next committed view. The run keeps every
//! fixture block it made in a [`Dag`], the independent model the cross
//! properties ([`crate::domain::cross`]) judge hashes against.
//!
//! Only the node is tapped: its reactor by a [`ReactorTap`] and its block
//! sync by a [`BlockTap`]. The harness registers both storages' operation
//! ID properties, block sync's resource and semantic properties, and the
//! cross properties.

use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use zakura_chain::block;
use zakura_header_chain::{BodyWorkAuthority, Frontier};
use zakura_network::zakura::{
    block_sync_serving_limit, framed_channel, CloseCauseReader, FramedRecv, FramedSend, Peer,
    Service, ServicePeerDirection, SessionOrigin, StreamReader, ZakuraBlockSyncConfig,
    ZakuraPeerId, ZakuraSupervisorHandle, ZAKURA_CAP_BLOCK_SYNC_REGULATED, ZAKURA_CAP_HEADER_SYNC,
    ZAKURA_HEADER_SYNC_STREAM_VERSION, ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
    ZAKURA_STREAM_BLOCK_SYNC, ZAKURA_STREAM_HEADER_SYNC,
};
use zakura_node_services::header_chain::{ApplyHeaderTargetOutcome, Port};
use zakura_rt::sync::{mpsc, CancellationToken};
use zakura_scheduler::{SchedulerConfig, SimInstant};
use zakura_storage::{
    block::{
        controlled::ControlledBlockService,
        fixture::{BlockFixture, BodySpec, FixtureBlock},
    },
    header::{adapter::EnginePort, controlled::ControlledPort},
};

use super::{
    block_node::{
        self, default_limits,
        policy::{self as block_policy, Predicates, Trigger},
        preload,
        relay::{Way, Wire},
        tap::{BlockTap, TapTeardown},
        view::BlockSyncView,
        BlockNode, LateHandle, MirrorReadSource, MirrorReads, MirrorStorage, NodeStorage,
    },
    block_sync::{
        storage_properties as block_storage_properties, storage_sources, table_wiring_properties,
    },
    header_node::{
        self,
        capacity::CapacityEvidence,
        claim::{ClaimEvidence, SettlementEvidence},
        reservation::ReservationEvidence,
        session::SessionEvidence,
        stale::StaleEvidence,
        tap::{ReactorTap, TapShutdown},
        DispatchFlag, HeaderNode, LINK_DEPTH,
    },
    header_sync::{check_service_operation_ids, Dialer},
    policy::LinkEffect,
    Subsystem,
};
use crate::{
    domain::{
        block_sync::{
            resource_properties, Limits, OwnerModel, OwnerView,
            TEARDOWN_RETURNS_BASELINE as BLOCK_TEARDOWN_RETURNS_BASELINE,
        },
        block_sync_semantic::{
            semantic_properties, SemanticModel, SemanticView, FRONTIER_WORK_CONSISTENT,
            QUERY_WORK_AUTHORIZED,
        },
        cross::{self, CrossModel, CrossView, Dag, ViewSample},
        header_sync::TeardownView,
    },
    property::{Capture, CheckCtx, HasView, Kind, Property, PropertyId, Source, Sources, Status},
    storage::HEADER_SERVICE,
    ActionSource, Boundary, Identity, ObservationLog, RunConfig, Sim,
};

/// The node's header operations get rising IDs, each completes at most once,
/// and only after its submission. The header storage harness's ID.
pub const HEADER_OPERATION_IDS: &str = super::header_sync::OPERATION_IDS;

/// A mirror: header sync and block sync on one engine that holds the whole
/// fixture chain. It's named `mirror-<i>` by its index in
/// [`SyncScenario::mirrors`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncMirror {
    /// Which end of its link to the node dials.
    pub dialer: Dialer,
}

/// A change the harness makes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SyncAction {
    /// Close the newest link to `peer`: both services' sessions on it.
    Close {
        /// A mirror's name.
        peer: String,
    },
    /// Link `peer` again, with a new connection and sessions, in its newest
    /// link's direction.
    Relink {
        /// A mirror's name.
        peer: String,
    },
    /// Extend every mirror's chain by `len` blocks from its tip: headers on
    /// its engine and bodies in its full state. The node must sync both.
    Grow {
        /// The new blocks.
        len: u32,
    },
    /// Switch every mirror to `fork`: headers on its engine and bodies in
    /// its full state. The node's header sync must take the fork's headers,
    /// and its block sync the fork's bodies.
    Reset {
        /// The fork.
        fork: block_node::control::Fork,
    },
    /// Stop the node: close every link, cancel its header-sync reactor, and
    /// tear its block sync down, as zakurad's shutdown does. Each tap expects
    /// its service to stop. A later relink fails.
    Teardown,
}

impl SyncAction {
    /// Who the action's identity names: a peer, `chain` for a change to
    /// every mirror's chain, or `node`.
    fn actor(&self) -> &str {
        match self {
            SyncAction::Close { peer } | SyncAction::Relink { peer } => peer,
            SyncAction::Grow { .. } | SyncAction::Reset { .. } => "chain",
            SyncAction::Teardown => "node",
        }
    }

    /// The action's name in its identity.
    fn name(&self) -> &'static str {
        match self {
            SyncAction::Close { .. } => "close",
            SyncAction::Relink { .. } => "relink",
            SyncAction::Grow { .. } => "grow",
            SyncAction::Reset { .. } => "reset",
            SyncAction::Teardown => "teardown",
        }
    }
}

/// One scripted [`SyncAction`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncStep {
    /// What to do.
    pub action: SyncAction,
    /// Offered only once this holds, over the node's block storage facts and
    /// the run's link and chain effects. Always offered without one.
    #[serde(default)]
    pub when: Option<Trigger>,
}

/// The `sync/storage` scenario.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncScenario {
    /// Blocks in the mirrors' chain above genesis. The node starts at
    /// genesis.
    pub blocks: u32,
    /// Each body's serialized size in bytes.
    pub body_bytes: u32,
    /// The mirrors, each linked once to the node at setup.
    pub mirrors: Vec<SyncMirror>,
    /// How the run completes the node's header operations.
    pub header_policy: header_node::Policy,
    /// How the run completes the node's block operations and the mirrors'
    /// range reads.
    pub block_policy: block_node::Policy,
    /// Scripted harness actions, offered in order.
    #[serde(default)]
    pub control: Vec<SyncStep>,
    /// The run's step budget. `None` sizes it to sync every block and then
    /// idle on timers.
    #[serde(default)]
    pub steps: Option<u64>,
}

impl Default for SyncScenario {
    /// Eight 2,000-byte blocks the node lacks, from one mirror that dials
    /// it, under both converging policies.
    fn default() -> Self {
        SyncScenario {
            blocks: 8,
            body_bytes: 2_000,
            mirrors: vec![SyncMirror {
                dialer: Dialer::Mirror,
            }],
            header_policy: header_node::Policy::converging(),
            block_policy: block_node::Policy::converging(),
            control: Vec::new(),
            steps: None,
        }
    }
}

impl SyncScenario {
    /// Each body's size and the size its header advertises.
    fn body_spec(&self) -> BodySpec {
        BodySpec {
            size: self.body_bytes,
            hint: self.body_bytes,
        }
    }

    /// The blocks every control step adds, which the step budget makes room
    /// for.
    pub(super) fn changed(&self) -> u64 {
        self.control
            .iter()
            .map(|step| match step.action {
                SyncAction::Grow { len } => u64::from(len),
                SyncAction::Reset { fork } => u64::from(fork.len),
                _ => 0,
            })
            .sum()
    }
}

/// The scenario's storage, with each mirror's chain committed before the
/// run. Cloning shares it, so a test can read it after a run.
#[derive(Clone)]
pub struct SyncPorts {
    /// The fixture every chain comes from.
    pub fixture: BlockFixture,
    /// The mirrors' chain above genesis when the run starts.
    pub chain: Vec<FixtureBlock>,
    /// The node's engine, which both of its services share.
    pub engine: Arc<EnginePort>,
    /// The node's header port over [`SyncPorts::engine`].
    pub port: Arc<ControlledPort>,
    /// The node's block service over [`SyncPorts::engine`].
    pub service: Arc<ControlledBlockService>,
    pub(crate) replies: Arc<LateHandle>,
    /// Each mirror's engine, in scenario order.
    pub mirrors: Vec<Arc<EnginePort>>,
    /// The mirrors' range reads.
    pub reads: MirrorReads,
    /// The node's block-sync owner state, as its tap folds it.
    pub view: Rc<RefCell<BlockSyncView>>,
    /// Every fixture block the run made, and the cross properties' model.
    pub cross: Rc<RefCell<CrossModel>>,
    /// The links made, and the chain changes, as the run's tasks made them.
    pub effects: block_node::Effects,
}

impl SyncPorts {
    /// The node's selected header tip and verified tip, read from its
    /// engine.
    pub fn tips(&self) -> (Frontier, Frontier) {
        let frontiers = self.engine.view().frontiers;
        (frontiers.header_best, frontiers.verified_best)
    }

    /// The mirrors' chain tip now, from the first mirror's engine.
    pub fn mirror_tip(&self) -> Frontier {
        self.mirrors
            .first()
            .map_or(self.fixture.anchor(), |engine| {
                engine.view().frontiers.header_best
            })
    }
}

/// One actor: header sync and block sync on one engine.
pub struct SyncNode {
    /// Its role name, `node` or `mirror-<i>`.
    pub name: String,
    /// Its header sync.
    pub header: HeaderNode,
    /// Its block sync.
    pub block: BlockNode,
}

/// One connection between the node under test and a mirror.
#[derive(Clone)]
struct SyncLink {
    /// The connection's block-sync side, which the block policy counts.
    block: block_node::Link,
    /// Each side's connection token, dialer first.
    tokens: [CancellationToken; 2],
}

impl SyncLink {
    /// Closes the connection on both sides.
    fn close(&self) {
        for token in &self.tokens {
            token.cancel();
        }
    }
}

/// Every actor in a run and the links between them.
pub struct SyncTopology {
    /// The actors, the node under test first.
    pub nodes: Vec<SyncNode>,
    links: RefCell<Vec<SyncLink>>,
    effects: block_node::Effects,
    /// The last connection and session ID handed out.
    last_id: Cell<u64>,
    /// The fixture, the body spec, and the mirrors' chain now.
    fixture: BlockFixture,
    spec: BodySpec,
    chain: RefCell<Vec<FixtureBlock>>,
    cross: Rc<RefCell<CrossModel>>,
    /// The relays that label the node's block-sync frames.
    wire: Wire,
    /// The node's taps, told of its teardown.
    header_tap: TapShutdown,
    block_tap: TapTeardown,
    /// What carries the links.
    transport: Transport,
}

/// What carries a run's links.
pub(crate) enum Transport {
    /// In-memory channels, the block side through the labelling relay
    /// (`sync/storage`).
    Memory,
    /// Real QUIC connections, which their dialers redial (`sync/wire`).
    Quic(QuicLinks),
}

/// The node's handle on its QUIC connections.
pub(crate) struct QuicLinks {
    /// The node's supervisor, once its endpoint serves.
    pub(crate) supervisor: Arc<std::sync::Mutex<Option<ZakuraSupervisorHandle>>>,
    /// Set once the node shuts down, so no dialer redials.
    pub(crate) stopped: Arc<std::sync::atomic::AtomicBool>,
    /// Each mirror's peer ID on QUIC, its endpoint key's, by name.
    pub(crate) peers: BTreeMap<String, ZakuraPeerId>,
}

impl SyncTopology {
    /// The topology of `actors`, with no link yet.
    pub(crate) fn new(
        actors: Actors,
        scenario: &SyncScenario,
        ports: &SyncPorts,
        transport: Transport,
    ) -> Self {
        SyncTopology {
            nodes: actors.nodes,
            links: RefCell::default(),
            effects: ports.effects.clone(),
            last_id: Cell::new(0),
            fixture: ports.fixture.clone(),
            spec: scenario.body_spec(),
            chain: RefCell::new(ports.chain.clone()),
            cross: ports.cross.clone(),
            wire: actors.wire,
            header_tap: actors.header_tap,
            block_tap: actors.block_tap,
            transport,
        }
    }

    /// The node's supervisor, if its links are QUIC connections and its
    /// endpoint serves.
    fn supervisor(&self) -> Option<ZakuraSupervisorHandle> {
        match &self.transport {
            Transport::Memory => None,
            Transport::Quic(quic) => quic
                .supervisor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        }
    }

    /// Closes the node's newest connection to the mirror named `peer`. A
    /// QUIC connection's dialer redials it.
    async fn close(&self, peer: &str) {
        let quic = match &self.transport {
            Transport::Memory => {
                match self.link_to(peer) {
                    Some(link) => link.close(),
                    None => self.fail(format!("the scenario never links `{peer}`")),
                }
                return;
            }
            Transport::Quic(quic) => quic,
        };
        let Some(id) = quic.peers.get(peer) else {
            self.fail(format!("the scenario has no mirror `{peer}`"));
            return;
        };
        let Some(supervisor) = self.supervisor() else {
            self.fail("the node's endpoint doesn't serve yet".to_owned());
            return;
        };
        if !supervisor.disconnect_peer(id).await {
            self.fail(format!("the node has no connection to `{peer}`"));
        }
    }

    /// The next connection and session ID.
    fn next_id(&self) -> u64 {
        let id = self.last_id.get() + 1;
        self.last_id.set(id);
        id
    }

    /// Records why a control step couldn't take effect, which ends the run
    /// as a harness failure.
    fn fail(&self, why: String) {
        self.effects.failure.borrow_mut().get_or_insert(why);
    }

    /// A channel for block-sync frames from `sender` to `receiver` on
    /// connection `id`, through the run's labelling relay: the sender's end,
    /// the receiver's, and the reader the node's side binds. One of them is
    /// the node under test.
    fn block_pipe(
        &self,
        id: u64,
        sender: usize,
        receiver: usize,
    ) -> (FramedSend, FramedRecv, Option<StreamReader>) {
        let way = if receiver == 0 {
            Way::ToNode
        } else {
            debug_assert_eq!(sender, 0, "one side of every link is the node under test");
            Way::FromNode
        };
        self.wire.pipe(id, way, LINK_DEPTH)
    }

    /// Connects `dialer` to `listener` with a new connection: one session
    /// of header sync's stream 5 and one of block sync's stream 6, each
    /// side's two sessions sharing its connection token, as one transport
    /// connection's do. One side must be the node under test.
    fn link(&self, dialer: usize, listener: usize) {
        let id = self.next_id();
        let tokens = [CancellationToken::new(), CancellationToken::new()];
        let (header_to_listener, header_from_dialer) = framed_channel(LINK_DEPTH);
        let (header_to_dialer, header_from_listener) = framed_channel(LINK_DEPTH);
        let (block_to_listener, block_from_dialer, listener_reader) =
            self.block_pipe(id, dialer, listener);
        let (block_to_dialer, block_from_listener, dialer_reader) =
            self.block_pipe(id, listener, dialer);
        let sides = [
            (
                dialer,
                listener,
                ServicePeerDirection::Outbound,
                (header_from_listener, header_to_listener),
                (block_from_listener, block_to_listener),
                dialer_reader,
                &tokens[0],
            ),
            (
                listener,
                dialer,
                ServicePeerDirection::Inbound,
                (header_from_dialer, header_to_dialer),
                (block_from_dialer, block_to_dialer),
                listener_reader,
                &tokens[1],
            ),
        ];
        let mut cause = None;
        for (local, remote, direction, header_streams, block_streams, reader, token) in sides {
            let remote_id = self.nodes[remote].header.id.clone();
            let header = Peer::new_versioned(
                id,
                remote_id.clone(),
                ZAKURA_CAP_HEADER_SYNC,
                direction,
                ZAKURA_STREAM_HEADER_SYNC,
                ZAKURA_HEADER_SYNC_STREAM_VERSION,
                id,
                header_streams,
                token.clone(),
            )
            .with_origin(SessionOrigin {
                connection: id,
                quic_stream: None,
                generation: id,
            });
            self.nodes[local].header.service().add_peer(header);
            let block = Peer::new_versioned(
                id,
                remote_id,
                ZAKURA_CAP_BLOCK_SYNC_REGULATED,
                direction,
                ZAKURA_STREAM_BLOCK_SYNC,
                ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
                id,
                block_streams,
                token.clone(),
            );
            if let Some(reader) = &reader {
                block.read_through(reader);
            }
            if local == 0 {
                cause = Some(block.close_cause_reader());
            }
            if !self.nodes[local].block.add_peer(block) {
                self.fail(format!(
                    "{} is torn down and takes no link",
                    self.nodes[local].name
                ));
            }
        }
        let cause: CloseCauseReader = cause.expect("one side of every link is the node");
        self.wire.watch(id, cause.clone());
        let peer = if dialer == 0 { listener } else { dialer };
        let block = block_node::Link::made(
            self.nodes[peer].name.clone(),
            dialer,
            listener,
            id,
            cause,
            tokens.clone(),
        );
        self.effects.links.borrow_mut().push(block.clone());
        self.links.borrow_mut().push(SyncLink { block, tokens });
    }

    /// The newest link to the mirror named `peer`.
    fn link_to(&self, peer: &str) -> Option<SyncLink> {
        self.links
            .borrow()
            .iter()
            .rev()
            .find(|link| link.block.peer == peer)
            .cloned()
    }

    /// Links the mirror named `peer` again, in its newest link's direction.
    /// A QUIC connection's dialer redials it by itself.
    fn relink(&self, peer: &str) {
        if let Transport::Quic(_) = self.transport {
            self.fail(format!(
                "`{peer}`'s dialer redials it; a wire run never relinks"
            ));
            return;
        }
        let Some(link) = self.link_to(peer) else {
            self.fail(format!("the scenario never links `{peer}`"));
            return;
        };
        let dialer = link.block.dialer.expect("a mirror link has a dialer");
        self.link(dialer, link.block.listener);
    }

    /// Stops the node under test, as zakurad's shutdown does: tells both
    /// taps, closes every link, cancels header sync's reactor and tears
    /// block sync down.
    async fn teardown(&self) {
        let node = &self.nodes[0];
        self.header_tap.begin();
        self.block_tap.begin();
        if let Transport::Quic(quic) = &self.transport {
            quic.stopped
                .store(true, std::sync::atomic::Ordering::SeqCst);
            if let Some(supervisor) = self.supervisor() {
                for id in quic.peers.values() {
                    supervisor.disconnect_peer(id).await;
                }
            }
        }
        // Cancelling can be a scheduling point, so the links are copied
        // first.
        let links = self.links.borrow().clone();
        for link in &links {
            link.close();
        }
        node.header.cancel();
        if !node.block.tear_down() {
            self.fail("the node is already torn down".to_owned());
        }
    }

    /// Plans the chain change `action` names, a `Grow` or a `Reset`, records
    /// its blocks in the DAG, and makes it the mirrors' chain. Records a
    /// failure, and returns `None`, if the change can't happen.
    fn plan_change(&self, action: &SyncAction) -> Option<(Frontier, Vec<FixtureBlock>)> {
        let chain = self.chain.borrow().clone();
        let tip = chain
            .last()
            .map_or(self.fixture.anchor(), FixtureBlock::frontier);
        let (parent, blocks) = match *action {
            SyncAction::Grow { len } => (tip, self.fixture.uniform(tip, len, self.spec, 0)),
            SyncAction::Reset { fork } => {
                if fork.at >= tip.height.0 || fork.at.saturating_add(fork.len) <= tip.height.0 {
                    self.fail(format!(
                        "a fork of {} blocks at {} doesn't branch below and outgrow the tip \
                         {tip:?}",
                        fork.len, fork.at
                    ));
                    return None;
                }
                let parent = if fork.at == 0 {
                    self.fixture.anchor()
                } else {
                    chain[usize::try_from(fork.at - 1).expect("fixture heights fit usize")]
                        .frontier()
                };
                (
                    parent,
                    self.fixture
                        .uniform(parent, fork.len, self.spec, fork.branch),
                )
            }
            _ => unreachable!("only `Grow` and `Reset` change the chain"),
        };
        self.cross.borrow_mut().dag.extend(parent, &blocks);
        let mut chain = self.chain.borrow_mut();
        chain.truncate(usize::try_from(parent.height.0).expect("fixture heights fit usize"));
        chain.extend(blocks.iter().cloned());
        Some((parent, blocks))
    }

    /// Makes the change on each mirror: its headers on the mirror's engine,
    /// and its bodies in the mirror's full state. Mirror tasks hold their
    /// engine's lock across scheduling points, so only a task may call
    /// this.
    async fn change_mirrors(&self, parent: Frontier, blocks: &[FixtureBlock]) {
        let target = blocks.last().expect("a chain change has blocks").frontier();
        for node in &self.nodes[1..] {
            let engine = node.block.engine.clone();
            let request = self
                .fixture
                .target_request(&engine.snapshot(), parent, blocks, 1);
            let applied = match engine.prepare_header_target(request).await {
                Ok(prepared) => engine.apply_header_target(prepared).await,
                Err(error) => Err(error),
            };
            let committed = engine
                .preload_blocks(blocks.iter().map(|block| block.block.clone()))
                .map_err(|error| format!("{error:?}"));
            let tip = engine.view().frontiers.verified_best;
            match (applied, committed) {
                (Ok(ApplyHeaderTargetOutcome::Applied), Ok(())) if tip == target => {
                    self.effects
                        .chains
                        .borrow_mut()
                        .push((LinkEffect::Grown, node.name.clone()));
                }
                (applied, committed) => self.fail(format!(
                    "moving {} to {target:?} gave {applied:?} and {committed:?}, with its tip at \
                     {tip:?}",
                    node.name
                )),
            }
        }
    }
}

/// Offers a scenario's scripted actions in order, each once, as
/// `peer:<actor>:<action>`. Closes, relinks and teardowns run on the link
/// task, and chain changes on the chain task.
pub(crate) struct Control {
    script: Vec<SyncStep>,
    topology: Rc<SyncTopology>,
    predicates: Rc<RefCell<Predicates>>,
    next: usize,
    to_links: mpsc::Sender<SyncAction>,
    to_chain: mpsc::Sender<(Frontier, Vec<FixtureBlock>)>,
}

impl Control {
    /// A source for `script` on `topology`, gated by `predicates`, with its
    /// tasks spawned on `sim`.
    pub(crate) fn new(
        sim: &mut Sim,
        topology: Rc<SyncTopology>,
        script: Vec<SyncStep>,
        predicates: Rc<RefCell<Predicates>>,
    ) -> Self {
        // Each channel holds the whole script.
        let (to_links, mut links) = mpsc::channel::<SyncAction>(script.len().max(1));
        let (to_chain, mut chain) =
            mpsc::channel::<(Frontier, Vec<FixtureBlock>)>(script.len().max(1));
        let task_topology = topology.clone();
        sim.scheduler().spawn_root("control", async move {
            while let Some(action) = links.recv().await {
                match action {
                    SyncAction::Close { peer } => task_topology.close(&peer).await,
                    SyncAction::Relink { peer } => task_topology.relink(&peer),
                    SyncAction::Teardown => task_topology.teardown().await,
                    action => unreachable!("the link task gets only links, not {action:?}"),
                }
            }
        });
        if script.iter().any(|step| {
            matches!(
                step.action,
                SyncAction::Grow { .. } | SyncAction::Reset { .. }
            )
        }) {
            let task_topology = topology.clone();
            sim.scheduler().spawn_root("control-chain", async move {
                while let Some((parent, blocks)) = chain.recv().await {
                    task_topology.change_mirrors(parent, &blocks).await;
                }
            });
        }
        Control {
            script,
            topology,
            predicates,
            next: 0,
            to_links,
            to_chain,
        }
    }
}

impl ActionSource for Control {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        let Some(step) = self.script.get(self.next) else {
            return;
        };
        let open = step
            .when
            .as_ref()
            .is_none_or(|when| self.predicates.borrow().control_holds(when, now));
        if open {
            // A script is far shorter than u64.
            out.push((
                Identity::peer(step.action.actor(), step.action.name()),
                self.next as u64,
            ));
        }
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        if let Some(when) = self
            .script
            .get(self.next)
            .and_then(|step| step.when.as_ref())
        {
            self.predicates.borrow().control_boundaries(when, now, out);
        }
    }

    fn execute(&mut self, _: u64, _: &mut ObservationLog) {
        let action = self.script[self.next].action.clone();
        self.next += 1;
        let full = "each channel holds the whole script, and its task outlives the run";
        match action {
            SyncAction::Grow { .. } | SyncAction::Reset { .. } => {
                if let Some(change) = self.topology.plan_change(&action) {
                    self.to_chain
                        .try_send(change)
                        .unwrap_or_else(|_| panic!("{full}"));
                }
            }
            action => self
                .to_links
                .try_send(action)
                .unwrap_or_else(|_| panic!("{full}")),
        }
    }
}

/// Watches the node's engine at each observation: hands the cross model the
/// engine's committed view and the block-sync semantic model its body-work
/// authority, and records a reorganization of its selected header chain as
/// the node's `Reset` chain effect, for the block policy's triggers.
/// It's added before the block policy's source, so a step's effect counts
/// in the same step.
struct EngineWatch {
    engine: Arc<EnginePort>,
    effects: block_node::Effects,
    cross: Rc<RefCell<CrossModel>>,
    /// The node's block-sync semantic model, whose query check expects the
    /// reactor to adopt the engine's newest view.
    semantic: Rc<RefCell<SemanticModel>>,
    /// Once the node's teardown begins, no reactor follows the engine.
    teardown: TapTeardown,
    /// The header tip at the last observation.
    header_best: Frontier,
}

impl ActionSource for EngineWatch {
    fn enabled(&self, _: SimInstant, _: &mut Vec<(Identity, u64)>) {}

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, _: u64, _: &mut ObservationLog) {
        unreachable!("the watch offers no action")
    }

    fn observe(&mut self, _: &mut ObservationLog) -> Result<(), String> {
        let view = self.engine.view();
        let authority = (!self.teardown.begun()).then(|| BodyWorkAuthority::for_view(&view).into());
        self.semantic.borrow_mut().authority.engine_view(authority);
        self.cross.borrow_mut().sample(ViewSample {
            state_version: view.state_version.get(),
            epoch: view.body_work_epoch.get(),
            header_best: view.frontiers.header_best,
            verified_best: view.frontiers.verified_best,
        });
        let header_best = view.frontiers.header_best;
        if header_best != self.header_best {
            let reorganized = !self
                .cross
                .borrow()
                .dag
                .descends(header_best, self.header_best);
            if reorganized {
                self.effects
                    .chains
                    .borrow_mut()
                    .push((LinkEffect::Reset, "node".to_owned()));
            }
            self.header_best = header_best;
        }
        Ok(())
    }
}

/// The node under test, named `node`, with header sync and block sync on
/// one engine, and mirror peers. See the module doc.
pub struct SyncStorage {
    view: SyncView,
    /// The node's block storage's replies, cleared once the run ends.
    replies: Arc<LateHandle>,
    /// The actors and links. They live until the run ends, so no reactor
    /// sees its inputs close unless a `Teardown` step stops the node.
    _topology: Rc<SyncTopology>,
}

/// What [`SyncStorage`]'s checks read between steps.
#[derive(Clone)]
pub struct SyncView {
    /// The node's block-sync owner state, as its tap folds it.
    pub block: Rc<RefCell<BlockSyncView>>,
    /// The cross properties' model.
    pub cross: Rc<RefCell<CrossModel>>,
    /// The node's header-sync settlement, as its reactor tap keeps it.
    pub header: HeaderSettlement,
}

/// The node's header-sync settlement evidence: the claim rules its reactor
/// tap found broken, and its released rounds and own teardown at each
/// drain. The tap reads the reactor's resource probe, so the teardown half
/// covers the chunk budget and the locator permits.
#[derive(Clone, Default)]
pub struct HeaderSettlement {
    claims: ClaimEvidence,
    rounds: SettlementEvidence,
}

impl TeardownView for SyncView {
    /// The header reactor's broken claim rules.
    fn claims(&self) -> &ClaimEvidence {
        &self.header.claims
    }

    /// The header reactor's released rounds and teardown.
    fn rounds(&self) -> &SettlementEvidence {
        &self.header.rounds
    }
}

impl SemanticView for SyncView {
    fn semantic(&self) -> Rc<RefCell<SemanticModel>> {
        self.block.borrow().semantic.clone()
    }
}

impl OwnerView for SyncView {
    fn read_owners<T>(&self, read: impl FnOnce(&OwnerModel) -> T) -> T {
        read(&self.block.borrow().owners)
    }
}

impl CrossView for SyncView {
    fn cross(&self) -> Rc<RefCell<CrossModel>> {
        self.cross.clone()
    }

    fn block(&self) -> Rc<RefCell<BlockSyncView>> {
        self.block.clone()
    }
}

impl HasView for SyncStorage {
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

/// Live steps a run allows per fixture block: block sync's few hundred, as
/// in `block-sync/storage`, and header sync's.
const STEPS_PER_BLOCK: u64 = 1_500;

/// The run's config for `scenario`: two workers, the default stack, and the
/// scenario's step budget, or room to sync every block, including those its
/// control steps add, and then idle on timers.
pub fn sync_config(scenario: &SyncScenario) -> RunConfig {
    RunConfig {
        scheduler: SchedulerConfig {
            workers: 2,
            ..SchedulerConfig::default()
        },
        budget: scenario.steps.unwrap_or(
            12_000 + STEPS_PER_BLOCK * (u64::from(scenario.blocks) + scenario.changed()),
        ),
        drain_bound: 2_000,
        ..RunConfig::default()
    }
}

impl Subsystem for SyncStorage {
    const NAME: &'static str = "sync/storage";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 2;
    type Scenario = SyncScenario;
    type Prepared = SyncPorts;

    /// [`sync_config`].
    fn config(scenario: &SyncScenario) -> RunConfig {
        sync_config(scenario)
    }

    /// Commits the mirrors' chain to each mirror's engine, headers and
    /// bodies, and builds the node's empty engine and both of its storages
    /// over it.
    ///
    /// # Panics
    ///
    /// If a hold or control step waits for a trigger the node never records
    /// ([`block_policy::undriven`]).
    fn prepare(scenario: &SyncScenario) -> SyncPorts {
        let triggers = scenario
            .block_policy
            .holds
            .iter()
            .map(|hold| &hold.until)
            .chain(
                scenario
                    .control
                    .iter()
                    .filter_map(|step| step.when.as_ref()),
            );
        if let Some(why) = triggers.filter_map(block_policy::undriven).next() {
            panic!("{why}");
        }
        let fixture = BlockFixture::regtest();
        let chain = fixture.uniform(fixture.anchor(), scenario.blocks, scenario.body_spec(), 0);
        let engine = Arc::new(EnginePort::new(fixture.headers()));
        let port = Arc::new(ControlledPort::over(engine.clone()));
        let replies = Arc::new(LateHandle::default());
        let service = Arc::new(ControlledBlockService::new(
            engine.clone(),
            replies.clone(),
            default_limits(),
        ));
        let mirrors = scenario
            .mirrors
            .iter()
            .map(|_| {
                let engine = Arc::new(EnginePort::new(fixture.headers()));
                preload(&engine, &fixture, &chain, chain.len());
                engine
            })
            .collect();
        let mut dag = Dag::new(fixture.anchor());
        dag.extend(fixture.anchor(), &chain);
        SyncPorts {
            fixture,
            chain,
            engine,
            port,
            service,
            replies,
            mirrors,
            reads: MirrorReads::default(),
            view: Rc::default(),
            cross: Rc::new(RefCell::new(CrossModel::new(dag))),
            effects: block_node::Effects::default(),
        }
    }

    /// Spawns the node's actors ([`spawn_actors`]), links each mirror to
    /// the node once, and adds the `Control` source.
    fn build(sim: &mut Sim, scenario: &SyncScenario, ports: SyncPorts) -> Self {
        let (actors, predicates) = spawn_actors(sim, scenario, &ports);
        let view = actors.view.clone();
        let header = actors.header.clone();
        let topology = Rc::new(SyncTopology::new(
            actors,
            scenario,
            &ports,
            Transport::Memory,
        ));
        for (index, spec) in (1..).zip(&scenario.mirrors) {
            match spec.dialer {
                Dialer::Mirror => topology.link(index, 0),
                Dialer::Node => topology.link(0, index),
            }
        }
        let control = Control::new(sim, topology.clone(), scenario.control.clone(), predicates);
        sim.add_source(Box::new(control));
        SyncStorage {
            view: SyncView {
                block: view,
                cross: ports.cross,
                header,
            },
            replies: ports.replies,
            _topology: topology,
        }
    }

    /// Both storages' operation ID properties, block sync's stream-6
    /// generated, resource and semantic properties, then the cross
    /// properties.
    fn properties() -> Vec<Property<Self>> {
        sync_properties()
    }

    /// Block sync's: the node's storage facts, its block-sync relays'
    /// frames, its closes and its view, plus the step loop's task
    /// observations ([`storage_sources`]).
    fn sources() -> Sources {
        storage_sources()
    }

    /// The saved scenario `prefix` tags ([`SavedSync::from_tag`]), or the
    /// default scenario.
    fn draw(prefix: &[u8]) -> SyncScenario {
        SavedSync::from_tag(prefix).map_or_else(SyncScenario::default, SavedSync::scenario)
    }
}

/// The run's actors, before any link: the node under test first, then
/// each mirror, and the node's taps and frame relays.
pub(crate) struct Actors {
    pub(crate) nodes: Vec<SyncNode>,
    /// The node's block-sync view, as its tap folds it.
    pub(crate) view: Rc<RefCell<BlockSyncView>>,
    /// The node's header-sync settlement, as its reactor tap keeps it.
    pub(crate) header: HeaderSettlement,
    wire: Wire,
    header_tap: TapShutdown,
    block_tap: TapTeardown,
}

/// Spawns the node's two services on its storages, each under its policy
/// and tapped, and the engine watch, then each mirror's services. Returns
/// the actors and the block policy's predicates, which control steps wait
/// on.
pub(crate) fn spawn_actors(
    sim: &mut Sim,
    scenario: &SyncScenario,
    ports: &SyncPorts,
) -> (Actors, Rc<RefCell<Predicates>>) {
    let anchor = ports.fixture.anchor();
    // The header side.
    let header_storage = header_node::PolicySource::new(
        sim,
        ports.port.clone(),
        scenario.header_policy.clone(),
        header_node::Effects::default(),
    );
    let rounds = SettlementEvidence::default();
    let (header_observer, header_tap) = ReactorTap::with_evidence(
        sim,
        &ports.port,
        "node",
        SessionEvidence::default(),
        ReservationEvidence::default(),
        CapacityEvidence::default(),
        rounds.clone(),
        StaleEvidence::default(),
    );
    let header = HeaderSettlement {
        claims: header_tap.claims(),
        rounds,
    };
    let header_shutdown = header_tap.shutdown();
    sim.add_source(Box::new(header_storage.with_tap(header_tap)));
    // The block side.
    let block_storage = block_node::PolicySource::new(
        sim,
        ports.service.clone(),
        scenario.block_policy.clone(),
        ports.effects.clone(),
    );
    let config = ZakuraBlockSyncConfig::default();
    let largest_body = ports
        .chain
        .iter()
        .map(FixtureBlock::size)
        .chain([scenario.body_bytes])
        .max()
        .map_or(0, u64::from);
    let workers = sync_config(scenario).scheduler.workers;
    let limits = Limits::of(&config, workers, largest_body);
    assert_eq!(
        limits.per_request, 1,
        "block-sync properties are checked only at one body per request"
    );
    ports.view.borrow_mut().owners = OwnerModel::new(limits, 0);
    let view = ports.view.clone();
    view.borrow().semantic.borrow_mut().set_limit(&config);
    let (block_observer, mut block_tap) = BlockTap::new(sim, &ports.service, "node", view.clone());
    let wire = Wire::new(block_sync_serving_limit(&config));
    block_tap.label_with(wire.clone());
    let block_teardown = block_tap.teardown();
    sim.add_source(Box::new(EngineWatch {
        engine: ports.engine.clone(),
        effects: ports.effects.clone(),
        cross: ports.cross.clone(),
        semantic: view.borrow().semantic.clone(),
        teardown: block_teardown.clone(),
        header_best: anchor,
    }));
    let sessions = block_tap.sessions();
    let block_storage = block_storage.with_tap(block_tap);
    let predicates = block_storage.predicates();
    sim.add_source(Box::new(block_storage));
    sim.add_source(Box::new(MirrorReadSource::new(
        ports.reads.clone(),
        predicates.clone(),
    )));
    let header_fixture = ports.fixture.headers();
    let dispatch = DispatchFlag::default();
    let node = SyncNode {
        name: "node".to_owned(),
        header: HeaderNode::spawn(
            sim,
            "node",
            1,
            ports.port.clone() as Arc<dyn Port>,
            header_fixture,
            anchor,
            dispatch.clone(),
            Some(header_observer),
        ),
        block: BlockNode::spawn(
            sim,
            "node",
            1,
            &ports.engine,
            NodeStorage::Controlled {
                service: ports.service.clone(),
                replies: ports.replies.clone(),
            },
            config,
            Some(block_observer),
        ),
    };
    sessions.watch(
        node.block
            .session_slots()
            .expect("a node just spawned holds its service"),
    );
    let tip = ports.chain.last().map_or(anchor, FixtureBlock::frontier);
    let mut nodes = vec![node];
    for ((index, engine), seed) in ports.mirrors.iter().enumerate().zip(2..) {
        let name = format!("mirror-{index}");
        nodes.push(SyncNode {
            header: HeaderNode::spawn(
                sim,
                &name,
                seed,
                engine.clone() as Arc<dyn Port>,
                header_fixture,
                tip,
                dispatch.clone(),
                None,
            ),
            block: BlockNode::spawn(
                sim,
                &name,
                seed,
                engine,
                NodeStorage::Mirror(MirrorStorage::new(
                    &name,
                    engine.clone(),
                    ports.reads.clone(),
                )),
                ZakuraBlockSyncConfig::default(),
                None,
            ),
            name,
        });
    }
    (
        Actors {
            nodes,
            view,
            header,
            wire,
            header_tap: header_shutdown,
            block_tap: block_teardown,
        },
        predicates,
    )
}

/// Checks [`HEADER_OPERATION_IDS`] on the node's header storage.
fn check_header_operation_ids<S: HasView>(cx: &mut CheckCtx<'_, S>) -> crate::property::Verdict {
    check_service_operation_ids(HEADER_SERVICE, cx)
}

/// Both storages' operation ID properties, block sync's stream-6
/// generated, resource and semantic properties, then the cross properties,
/// for a harness whose view is a [`SyncView`].
pub(crate) fn sync_properties<S: HasView<View = SyncView>>() -> Vec<Property<S>> {
    let mut properties = vec![Property {
        id: PropertyId::new(HEADER_OPERATION_IDS),
        claim: "the node's storage operations get rising IDs, and each completes at most \
                once, after its submission",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::Storage],
        status: Status::Active,
        check: check_header_operation_ids::<S>,
        witness: None,
    }];
    properties.extend(block_storage_properties());
    properties.extend(table_wiring_properties());
    properties.extend(resource_properties());
    properties.extend(semantic_properties());
    properties.extend(cross::storage_properties());
    properties.extend(cross::teardown_properties());
    properties
}

/// A saved scenario of `sync/storage`. Each has a committed input under
/// `tests/fixtures/sync-storage-<name>.bin`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SavedSync {
    /// Eight blocks from two mirrors, one dialing each way.
    Honest,
    /// The mirrors switch to a fork from height 4 once the node has asked
    /// storage to admit four bodies. The node's header sync takes the fork,
    /// and its block sync downloads the fork's bodies.
    Reorg,
    /// The mirrors' chain grows by four blocks once the node's first
    /// needed-block query reaches storage, which holds that query for
    /// [`RETIRED_QUERY_HOLD_MS`]. Virtual time moves only once no task can
    /// run, so by then the reactor has adopted the longer chain's committed
    /// view. Its new header target retires the query under the same
    /// body-work epoch, and the query's reply reaches the reactor under the
    /// retired authority.
    RetiredQuery,
    /// The node shuts down while storage holds its first needed-block
    /// query, which completes [`TEARDOWN_HOLD_MS`] after its submission.
    TeardownQuery,
    /// The node shuts down while storage holds its second body's apply,
    /// which completes [`TEARDOWN_HOLD_MS`] after its submission.
    TeardownSubmission,
    /// The `reorg` fork reaches the node while storage holds its first
    /// needed-block query for [`REORG_HOLD_MS`]: the query stage.
    ReorgQuery,
    /// The `reorg` fork reaches the node while a mirror holds its
    /// [`REORG_READ_NTH`]th range read for [`REORG_HOLD_MS`], so a request
    /// the node published under the old epoch is outstanding when the
    /// sequencer applies the new one, and its body arrives after the reset:
    /// the request and receive stages.
    ReorgRequest,
    /// The `reorg` fork reaches the node while storage holds its
    /// [`REORG_NTH`]th queued submission for [`REORG_HOLD_MS`] before
    /// admitting it: the submit stage.
    ReorgSubmit,
    /// The `reorg` fork reaches the node while storage holds its
    /// [`REORG_NTH`]th admitted apply for [`REORG_HOLD_MS`] before committing
    /// it: the verdict stage.
    ReorgVerdict,
}

/// How long the teardown scenarios hold their operation past the node's
/// shutdown, at least, in milliseconds of virtual time.
const TEARDOWN_HOLD_MS: u64 = 2_000;

/// How long [`SavedSync::RetiredQuery`] holds its query, at least, in
/// milliseconds of virtual time.
const RETIRED_QUERY_HOLD_MS: u64 = 2_000;

/// How long the reorg stage scenarios hold their operation, at least, in
/// milliseconds of virtual time.
const REORG_HOLD_MS: u64 = 2_000;

/// Which of its kind's operations a reorg stage scenario holds: one above
/// the fork point at height 4.
const REORG_NTH: u32 = 6;

/// Which mirror range read [`SavedSync::ReorgRequest`] holds: an early one,
/// at a height every branch shares, whose request the epoch change still
/// retires because it lies above the verified tip.
const REORG_READ_NTH: u32 = 2;

/// The first byte of every saved scenario's tag.
const TAG: u8 = 0xFD;

impl SavedSync {
    /// Every saved scenario.
    pub const ALL: [SavedSync; 9] = [
        SavedSync::Honest,
        SavedSync::Reorg,
        SavedSync::RetiredQuery,
        SavedSync::TeardownQuery,
        SavedSync::TeardownSubmission,
        SavedSync::ReorgQuery,
        SavedSync::ReorgRequest,
        SavedSync::ReorgSubmit,
        SavedSync::ReorgVerdict,
    ];

    /// The fixture's name, `sync-storage-<name>.bin`.
    pub fn name(self) -> &'static str {
        match self {
            SavedSync::Honest => "honest",
            SavedSync::Reorg => "reorg",
            SavedSync::RetiredQuery => "retired-query",
            SavedSync::TeardownQuery => "teardown-query",
            SavedSync::TeardownSubmission => "teardown-submission",
            SavedSync::ReorgQuery => "reorg-query",
            SavedSync::ReorgRequest => "reorg-request",
            SavedSync::ReorgSubmit => "reorg-submit",
            SavedSync::ReorgVerdict => "reorg-verdict",
        }
    }

    /// The two-byte prefix that picks it: `0xFD`, then its place in
    /// [`SavedSync::ALL`] from 1.
    pub fn tag(self) -> [u8; 2] {
        let index = SavedSync::ALL
            .iter()
            .position(|saved| *saved == self)
            .expect("every saved scenario is listed");
        // A handful of scenarios fit a byte.
        [TAG, (index + 1) as u8]
    }

    /// The properties whose trigger history its committed input reaches,
    /// each with the outcome the unpatched run gives, for its sidecar's
    /// `[[witness]]` entries, which `cargo xtask planted` checks.
    pub fn witnesses(self) -> &'static [(&'static str, &'static str)] {
        match self {
            SavedSync::Honest => &[],
            SavedSync::Reorg => &[
                (FRONTIER_WORK_CONSISTENT, "holds"),
                (cross::TIPS_CONSISTENT, "holds"),
                (cross::BLOCK_REQUESTS_FOLLOW_HEADERS, "holds"),
                (cross::HEADER_REORG_RETIRES_BLOCK_WORK, "holds"),
            ],
            SavedSync::ReorgQuery
            | SavedSync::ReorgRequest
            | SavedSync::ReorgSubmit
            | SavedSync::ReorgVerdict => &[
                (cross::BLOCK_REQUESTS_FOLLOW_HEADERS, "holds"),
                (cross::HEADER_REORG_RETIRES_BLOCK_WORK, "holds"),
            ],
            SavedSync::RetiredQuery => &[(QUERY_WORK_AUTHORIZED, "holds")],
            SavedSync::TeardownQuery | SavedSync::TeardownSubmission => &[
                (BLOCK_TEARDOWN_RETURNS_BASELINE, "holds"),
                (cross::TEARDOWN_RETURNS_BASELINE, "holds"),
            ],
        }
    }

    /// The saved scenario `prefix` tags, if any.
    pub fn from_tag(prefix: &[u8]) -> Option<SavedSync> {
        SavedSync::ALL
            .into_iter()
            .find(|saved| prefix.get(..2) == Some(&saved.tag()[..]))
    }

    /// Its scenario.
    pub fn scenario(self) -> SyncScenario {
        let two_mirrors = SyncScenario {
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
        let fork = SyncAction::Reset {
            fork: block_node::control::Fork {
                branch: 1,
                at: 4,
                len: 8,
            },
        };
        match self {
            SavedSync::Honest => two_mirrors,
            SavedSync::Reorg => SyncScenario {
                control: vec![SyncStep {
                    action: fork,
                    when: Some(admitted(4)),
                }],
                ..two_mirrors
            },
            SavedSync::RetiredQuery => {
                let query = block_policy::Select::nth(block_policy::BlockKind::Query, 1);
                let mut block_policy = block_node::Policy::converging();
                block_policy.holds.push(block_policy::Hold {
                    select: query.clone(),
                    until: Trigger::VirtualTime(RETIRED_QUERY_HOLD_MS),
                    then: vec![block_policy::BlockAllow::Answer],
                    deadline_ms: None,
                });
                SyncScenario {
                    block_policy,
                    control: vec![SyncStep {
                        action: SyncAction::Grow { len: 4 },
                        when: Some(Trigger::Submitted(query)),
                    }],
                    ..two_mirrors
                }
            }
            SavedSync::TeardownQuery => teardown_holding(
                two_mirrors,
                block_policy::Select::nth(block_policy::BlockKind::Query, 1),
            ),
            SavedSync::TeardownSubmission => teardown_holding(
                two_mirrors,
                block_policy::Select::nth(block_policy::BlockKind::Apply, 2),
            ),
            SavedSync::ReorgQuery => reorg_holding(
                two_mirrors,
                fork,
                block_policy::Select::nth(block_policy::BlockKind::Query, 1),
                vec![block_policy::BlockAllow::Answer],
            ),
            SavedSync::ReorgRequest => reorg_holding(
                two_mirrors,
                fork,
                block_policy::Select::nth(block_policy::BlockKind::MirrorRead, REORG_READ_NTH),
                vec![block_policy::BlockAllow::Serve],
            ),
            SavedSync::ReorgSubmit => reorg_holding(
                two_mirrors,
                fork,
                block_policy::Select::nth(block_policy::BlockKind::Admit, REORG_NTH),
                vec![
                    block_policy::BlockAllow::Admit,
                    block_policy::BlockAllow::Committed,
                ],
            ),
            SavedSync::ReorgVerdict => reorg_holding(
                two_mirrors,
                fork,
                block_policy::Select::nth(block_policy::BlockKind::Apply, REORG_NTH),
                vec![block_policy::BlockAllow::Committed],
            ),
        }
    }
}

/// `scenario`, with the operation `held` selects held for
/// [`REORG_HOLD_MS`] and then offered only `then`, and the mirrors switching
/// to `fork` once storage or a mirror takes it.
fn reorg_holding(
    scenario: SyncScenario,
    fork: SyncAction,
    held: block_policy::Select,
    then: Vec<block_policy::BlockAllow>,
) -> SyncScenario {
    let mut block_policy = block_node::Policy::converging();
    block_policy.holds.push(block_policy::Hold {
        select: held.clone(),
        until: Trigger::VirtualTime(REORG_HOLD_MS),
        then,
        deadline_ms: None,
    });
    SyncScenario {
        block_policy,
        control: vec![SyncStep {
            action: fork,
            when: Some(Trigger::Submitted(held)),
        }],
        ..scenario
    }
}

/// `scenario`, with block storage holding the operation `held` selects for
/// [`TEARDOWN_HOLD_MS`], and the node shutting down once storage takes it.
fn teardown_holding(scenario: SyncScenario, held: block_policy::Select) -> SyncScenario {
    let mut block_policy = block_node::Policy::converging();
    block_policy.holds.push(block_policy::Hold {
        select: held.clone(),
        until: Trigger::VirtualTime(TEARDOWN_HOLD_MS),
        then: vec![
            block_policy::BlockAllow::Answer,
            block_policy::BlockAllow::Committed,
        ],
        deadline_ms: None,
    });
    SyncScenario {
        block_policy,
        control: vec![SyncStep {
            action: SyncAction::Teardown,
            when: Some(Trigger::Submitted(held)),
        }],
        ..scenario
    }
}

/// The trigger that holds once the node has asked block storage to admit
/// `count` bodies.
pub fn admitted(count: u32) -> Trigger {
    Trigger::Count(
        count,
        Box::new(Trigger::Submitted(block_policy::Select {
            kind: Some(block_policy::BlockKind::Admit),
            ..block_policy::Select::default()
        })),
    )
}

/// `height` as a block height, for tests.
pub fn height(height: u32) -> block::Height {
    block::Height(height)
}

/// Each fixture block's hash by height on the chain `blocks`, for tests.
pub fn hashes(blocks: &[FixtureBlock]) -> BTreeMap<u32, String> {
    blocks
        .iter()
        .map(|block| (block.height.0, crate::storage::hash(block.hash())))
        .collect()
}
