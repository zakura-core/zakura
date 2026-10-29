//! The `block-sync/storage` harness: a block-sync node on controlled block
//! storage, with mirror peers that hold the whole fixture chain.
//!
//! The node's real reactor, routines and sequencer run against a
//! [`ControlledBlockService`] that shares its engine with the node's header
//! side, as zakurad's driver shares zakura-state with header sync. Its
//! serving reads through the same service, but only a probe asks it for a
//! body. Header sync itself doesn't run: every actor's headers are
//! committed before the run, so block sync's work comes only from the
//! committed views.
//!
//! The scenario leaves out what the service doesn't model: zakurad's legacy
//! handoff and `SyncCoordinator` phases, the debug throughput probe, stall
//! traces, the fail-closed shutdown when an invalid-body record isn't
//! durable, the 30 s timeouts on state calls, and the tampered-body
//! disconnect. No block property covers them.
//!
//! Every link carries a [`Wire`] relay that labels each frame with the
//! stream-6 role models, and the node's tap writes its identities and
//! effects beside the labels, so the harness checks stream 6's generated
//! receiver, downstream and sender properties.

use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use zakura_network::zakura::{block_sync_serving_limit, block_sync_streams, ZakuraBlockSyncConfig};
use zakura_scheduler::{At, SchedulerConfig, TaskId};
use zakura_storage::{
    block::{
        controlled::ControlledBlockService,
        fixture::{BlockFixture, BodySpec, FixtureBlock},
    },
    header::adapter::EnginePort,
};

use super::{
    block_node::{
        control::{Control, ControlAction, ControlStep, Fork},
        default_limits, policy, preload,
        probe::ProbeLog,
        relay::Wire,
        tap::{BlockTap, TapHandoffs},
        view::BlockSyncView,
        BlockNode, Effects, LateHandle, LiveChain, MirrorReadSource, MirrorReads, MirrorStorage,
        NodeStorage, Policy, PolicySource, Topology,
    },
    header_sync::{check_service_operation_ids, Dialer},
    Subsystem,
};
use crate::{
    domain::{
        block_sync::{resource_properties, Limits, OwnerModel, OwnerView},
        block_sync_semantic::{semantic_properties, SemanticModel, SemanticView},
    },
    property::{
        Capture, CheckCtx, HasView, Kind, Property, PropertyId, Source, Sources, Status, Verdict,
    },
    storage::BLOCK_SERVICE,
    table::{downstream_properties, sender_properties, table_properties, Effect},
    RunConfig, Sim, TaskHold,
};

/// The node's block operations get rising IDs, each completes at most once,
/// and only after its submission.
pub const OPERATION_IDS: &str = "block-sync/storage-operation-ids";

/// The family the stream-6 generated property IDs name.
pub const FAMILY: &str = "block-sync";

/// A mirror peer: real block-sync code on full state that holds the whole
/// fixture chain. It's named `mirror-<i>` by its index in
/// [`BlockScenario::mirrors`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MirrorSpec {
    /// Which end of its link to the node dials.
    pub dialer: Dialer,
    /// Heights at which the run never serves a range read that starts
    /// there. Each such read waits until block sync drops it, so the node
    /// never gets those reads' bodies from this mirror.
    #[serde(default)]
    pub withheld: Vec<u32>,
}

impl MirrorSpec {
    /// A mirror that serves every height, linked by `dialer`.
    pub fn new(dialer: Dialer) -> Self {
        MirrorSpec {
            dialer,
            withheld: Vec::new(),
        }
    }
}

/// The `block-sync/storage` scenario.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockScenario {
    /// Blocks in the fixture chain above genesis. Every actor holds their
    /// headers.
    pub blocks: u32,
    /// Each body's serialized size in bytes.
    pub body_bytes: u32,
    /// The size each header advertises, 0 for none, which block sync reads
    /// as unknown and reserves the most a body can be. `None` advertises
    /// each body's exact size.
    #[serde(default)]
    pub size_hint: Option<u32>,
    /// The bodies the node holds when the run starts, from height 1.
    #[serde(default)]
    pub node_bodies: u32,
    /// The mirror peers, each linked once to the node at setup.
    pub mirrors: Vec<MirrorSpec>,
    /// Whether a scripted probe links to the node at setup, after the
    /// mirrors. Only `Control` steps make it send anything.
    #[serde(default)]
    pub probe: bool,
    /// The node's configured limit on the requests a peer may have in
    /// flight, which caps what it advertises and serves. It also caps the
    /// node's own outbound requests per peer and its refill target. `None`
    /// keeps production's.
    #[serde(default)]
    pub serving_limit: Option<u32>,
    /// The node's byte budget for outstanding requests, `M`
    /// (`max_inflight_block_bytes`). `None` keeps production's.
    #[serde(default)]
    pub max_inflight_block_bytes: Option<u64>,
    /// The node's look-ahead limit, `L` (`max_reorder_lookahead_bytes`).
    /// `None` keeps production's. The node takes it as given: zakurad's
    /// config loading would clamp it up to one checkpoint range of
    /// maximum-size bodies.
    #[serde(default)]
    pub max_reorder_lookahead_bytes: Option<u64>,
    /// The most block applies the node keeps submitted, which sets the
    /// sequencer's input queue depth `Q`
    /// (`max_submitted_block_applies`). `None` keeps production's.
    #[serde(default)]
    pub max_submitted_block_applies: Option<usize>,
    /// The response byte target the node advertises
    /// (`max_response_bytes`), which sets the smallest budget its config
    /// may have. `None` keeps production's.
    #[serde(default)]
    pub max_response_bytes: Option<u32>,
    /// The run's step budget. `None` sizes it to sync every block and then
    /// idle on timers.
    #[serde(default)]
    pub steps: Option<u64>,
    /// The runtime's workers, `W`. `None` runs 2.
    #[serde(default)]
    pub workers: Option<u16>,
    /// How the run completes the node's block operations and the mirrors'
    /// range reads.
    pub policy: Policy,
    /// Scripted harness actions, offered in order.
    #[serde(default)]
    pub control: Vec<ControlStep>,
    /// If set, the run pauses routines in their handoffs to the sequencer.
    #[serde(default)]
    pub handoff_pause: Option<HandoffPause>,
}

/// Pauses each routine that hands a body to the sequencer, after the send
/// and before its budget release, so the sequencer can drain the body and
/// move the download floor while the budget still counts it (D2's `h`).
///
/// The pauses end, for good, once the byte budget counts more than
/// `M + 2·k·B`, or `steps` steps after the first one began. A paused
/// routine also runs whenever nothing else can, since a [`TaskHold`] never
/// ends a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandoffPause {
    /// The lowest height whose handoff pauses.
    pub from_height: u32,
    /// The most steps the pauses last after the first one began.
    pub steps: u64,
}

impl Default for BlockScenario {
    /// Eight 2,000-byte blocks the node lacks, from one mirror that dials
    /// it, under the converging policy.
    fn default() -> Self {
        BlockScenario {
            blocks: 8,
            body_bytes: 2_000,
            size_hint: None,
            node_bodies: 0,
            mirrors: vec![MirrorSpec::new(Dialer::Mirror)],
            probe: false,
            serving_limit: None,
            max_inflight_block_bytes: None,
            max_reorder_lookahead_bytes: None,
            max_submitted_block_applies: None,
            max_response_bytes: None,
            steps: None,
            workers: None,
            policy: Policy::converging(),
            control: Vec::new(),
            handoff_pause: None,
        }
    }
}

impl BlockScenario {
    /// Sixteen bodies whose headers advertise no size, from four mirrors.
    /// Each request reserves the most a body can be, `B`, against a byte
    /// budget of `1.5·B`, so one request leaves the budget short of the
    /// next. Only a floor request may then charge past it, but no run has
    /// been seen to: the floor request's own reservation fits first. The
    /// node advertises a response target of `B`, so zakurad's validation
    /// accepts that budget.
    pub fn short_budget() -> Self {
        BlockScenario {
            blocks: 16,
            size_hint: Some(0),
            mirrors: vec![
                MirrorSpec::new(Dialer::Mirror),
                MirrorSpec::new(Dialer::Node),
                MirrorSpec::new(Dialer::Mirror),
                MirrorSpec::new(Dialer::Node),
            ],
            max_inflight_block_bytes: Some(3_000_000),
            max_response_bytes: Some(2_000_000),
            ..BlockScenario::default()
        }
    }

    /// 840 bodies from one mirror that withholds height 3, so nothing
    /// commits past height 2 and the sequencer keeps every body it admits.
    /// The mirror serves heights 1 and 2 first, so the node's probe request
    /// finds it live and keeps asking. The look-ahead limit `L` is 4,000
    /// bytes, so the node fetches only the 400 heights the commit window
    /// exempts above the floor. 840 bodies are more than `L + 401·b + Q·b`
    /// allows. The window fills in about 340,000 steps. A node whose window
    /// let it keep 805 bodies would pass the bound near step 1,060,000, so
    /// the run stops at 1,500,000.
    pub fn stuck_floor() -> Self {
        BlockScenario {
            blocks: 840,
            mirrors: vec![MirrorSpec {
                dialer: Dialer::Mirror,
                withheld: vec![3],
            }],
            max_reorder_lookahead_bytes: Some(4_000),
            steps: Some(1_500_000),
            ..BlockScenario::default()
        }
    }

    /// The short budget on 4 workers, with each routine paused in its
    /// handoff from height 5, by which point each mirror has delivered a
    /// body. A paused routine's body moves the download floor while the
    /// budget still counts it, so the next routine's floor request charges
    /// past `M`. Every estimate is `B` and `M = 1.5·B`, so the budget passes
    /// `M + 2·B` once three routines are paused and the fourth charges. Each
    /// paused routine keeps its worker, so the run needs more than 2.
    pub fn paused_handoffs() -> Self {
        BlockScenario {
            workers: Some(4),
            handoff_pause: Some(HandoffPause {
                from_height: 5,
                steps: 20_000,
            }),
            ..BlockScenario::short_budget()
        }
    }

    /// 420 bodies of unknown size from four mirrors that each withhold
    /// height 3 and every height above 403, so nothing commits past height
    /// 2 and every above-window request stays outstanding. The byte budget
    /// `M` is `10·B` and the look-ahead limit `L` is `2.5·B`, more than the
    /// 400 bodies the commit window retains. Once the window's heights
    /// arrive, the node requests heights above it until its estimates spend
    /// `L`.
    pub fn above_window() -> Self {
        BlockScenario {
            blocks: 420,
            size_hint: Some(0),
            mirrors: [Dialer::Mirror, Dialer::Node, Dialer::Mirror, Dialer::Node]
                .into_iter()
                .map(|dialer| MirrorSpec {
                    dialer,
                    withheld: [3].into_iter().chain(404..=420).collect(),
                })
                .collect(),
            max_inflight_block_bytes: Some(20_000_000),
            max_reorder_lookahead_bytes: Some(5_000_000),
            max_response_bytes: Some(2_000_000),
            steps: Some(300_000),
            ..BlockScenario::default()
        }
    }

    /// The paused handoffs on 24 bodies, with the floor's other churn
    /// (properties.md, P5.3): every mirror withholds height 7, so each
    /// request for it times out; `mirror-3`'s link closes once the node has
    /// asked storage to admit four bodies, which retires its routine; and
    /// the chain resets to a fork from height 12 at 12 s of virtual time,
    /// after the 8 s request timeout.
    pub fn floor_churn() -> Self {
        let after = |submissions| {
            Some(policy::Trigger::Count(
                submissions,
                Box::new(policy::Trigger::Submitted(policy::Select {
                    kind: Some(policy::BlockKind::Admit),
                    ..policy::Select::default()
                })),
            ))
        };
        let mut scenario = BlockScenario {
            blocks: 24,
            control: vec![
                ControlStep {
                    action: ControlAction::Close {
                        peer: "mirror-3".to_owned(),
                    },
                    when: after(4),
                },
                ControlStep {
                    action: ControlAction::Reset {
                        fork: Fork {
                            branch: 1,
                            at: 12,
                            len: 16,
                        },
                        node_first: false,
                        apply_when: None,
                    },
                    when: Some(policy::Trigger::VirtualTime(12_000)),
                },
            ],
            ..BlockScenario::paused_handoffs()
        };
        for mirror in &mut scenario.mirrors {
            mirror.withheld = vec![7];
        }
        scenario
    }

    /// Two mirrors serve sixteen bodies but withhold every height from 9, and
    /// both links close once the node has asked storage to admit eight
    /// bodies. Every routine ends owing requests it must return, and then
    /// every peer has left.
    pub fn peers_leave() -> Self {
        let after_eight = Some(policy::Trigger::Count(
            8,
            Box::new(policy::Trigger::Submitted(policy::Select {
                kind: Some(policy::BlockKind::Admit),
                ..policy::Select::default()
            })),
        ));
        BlockScenario {
            blocks: 16,
            mirrors: [Dialer::Mirror, Dialer::Node]
                .into_iter()
                .map(|dialer| MirrorSpec {
                    dialer,
                    withheld: (9..=16).collect(),
                })
                .collect(),
            control: ["mirror-0", "mirror-1"]
                .into_iter()
                .map(|peer| ControlStep {
                    action: ControlAction::Close {
                        peer: peer.to_owned(),
                    },
                    when: after_eight.clone(),
                })
                .collect(),
            ..BlockScenario::default()
        }
    }

    /// [`above_window`](Self::above_window), but every header advertises
    /// 1,024 bytes, the smallest estimate, for its 2,000-byte body, and the
    /// mirrors withhold only height 3, so above-window bodies arrive larger
    /// than their requests' estimates while the sequencer can't commit
    /// (properties.md, P5.4). Admission counts every retained body against
    /// the look-ahead limit `L`, the commit window's too, so with `L` at
    /// 1,000,000 bytes the 400 window bodies leave room for about 100
    /// above-window heights. Each peer serves at most `outstanding` requests
    /// at once, and the input queue holds `queue` bodies, at least 401. With
    /// one request at a time, the above-window bodies arrive within 600,000
    /// steps.
    pub fn understated_hints(outstanding: u32, queue: usize) -> Self {
        let mut scenario = BlockScenario {
            blocks: 520,
            size_hint: Some(1_024),
            serving_limit: Some(outstanding),
            max_reorder_lookahead_bytes: Some(1_000_000),
            max_submitted_block_applies: Some(queue),
            steps: Some(600_000),
            ..BlockScenario::above_window()
        };
        for mirror in &mut scenario.mirrors {
            mirror.withheld = vec![3];
        }
        scenario
    }

    /// The node's block-sync config: production's, with the scenario's
    /// limits.
    ///
    /// # Panics
    ///
    /// If zakurad's validation refuses it.
    pub fn node_config(&self) -> ZakuraBlockSyncConfig {
        let mut config = ZakuraBlockSyncConfig::default();
        if let Some(limit) = self.serving_limit {
            config.max_inflight_requests = limit;
        }
        if let Some(bytes) = self.max_inflight_block_bytes {
            config.max_inflight_block_bytes = bytes;
        }
        if let Some(bytes) = self.max_reorder_lookahead_bytes {
            config.max_reorder_lookahead_bytes = bytes;
        }
        if let Some(bytes) = self.max_response_bytes {
            config.max_response_bytes = bytes;
        }
        if let Some(applies) = self.max_submitted_block_applies {
            config.max_submitted_block_applies = applies;
        }
        if let Err(error) = config.validate() {
            panic!("the scenario's node config is invalid: {error}");
        }
        config
    }

    /// Each body's size and the size its header advertises.
    fn body_spec(&self) -> BodySpec {
        BodySpec {
            size: self.body_bytes,
            hint: self.size_hint.unwrap_or(self.body_bytes),
        }
    }
}

/// The scenario's storage, with each actor's chain committed before the
/// run. Cloning shares it, so a test can read it after a run.
#[derive(Clone)]
pub struct Ports {
    /// The fixture every chain comes from.
    pub fixture: BlockFixture,
    /// The fixture chain above genesis.
    pub chain: Vec<FixtureBlock>,
    /// The node's engine, which its block service shares.
    pub node: Arc<EnginePort>,
    /// The node's block service.
    pub service: Arc<ControlledBlockService>,
    replies: Arc<LateHandle>,
    /// Each mirror's engine, in scenario order.
    pub mirrors: Vec<Arc<EnginePort>>,
    /// The chain as `Control` steps grow and fork it, which starts as
    /// `chain`.
    pub live: LiveChain,
    /// The mirrors' range reads.
    pub reads: MirrorReads,
    /// What the node sent the probe, if the scenario links one.
    pub probe: ProbeLog,
    /// The links made and the chain changes, as the run's tasks made them.
    pub effects: Effects,
    /// The node's block-sync owner state, as its tap folds it.
    pub view: Rc<RefCell<BlockSyncView>>,
}

/// The node under test, named `node`, on a controlled block service with
/// mirror peers, checked by [`storage_properties`] and the resource
/// properties ([`resource_properties`]). Only the node's block sync is
/// tapped.
pub struct Storage {
    /// The node's owner state, as its tap folds it.
    view: Rc<RefCell<BlockSyncView>>,
    /// The nodes and links. They live until the run ends, so no reactor sees
    /// its inputs close unless a `Teardown` step drops the node's.
    _topology: Rc<Topology>,
}

impl HasView for Storage {
    type View = Rc<RefCell<BlockSyncView>>;

    /// The node's owner state as of the tap's last observation, shared
    /// rather than copied, since it grows with the chain. The tap changes it
    /// only when it observes, which no check outlives.
    fn view(&self) -> Rc<RefCell<BlockSyncView>> {
        self.view.clone()
    }

    /// The step of the tap's last observation, or `Missing` before its
    /// first.
    fn captured(&self) -> Capture {
        self.view
            .borrow()
            .step
            .map_or(Capture::Missing, Capture::AfterStep)
    }
}

impl Subsystem for Storage {
    const NAME: &'static str = "block-sync/storage";
    const GENERATOR: u32 = 2;
    const SCENARIO_BYTES: usize = 2;
    type Scenario = BlockScenario;
    type Prepared = Ports;

    /// [`storage_config`].
    fn config(scenario: &BlockScenario) -> RunConfig {
        storage_config(scenario)
    }

    /// Commits every actor's headers, and each mirror's bodies, to its
    /// engine.
    ///
    /// # Panics
    ///
    /// If the node holds more bodies than the scenario has blocks, or a
    /// hold or control step waits for a trigger the node never records
    /// ([`policy::undriven`]).
    fn prepare(scenario: &BlockScenario) -> Ports {
        assert!(
            scenario.node_bodies <= scenario.blocks,
            "a node holds at most the scenario's {} blocks, not {}",
            scenario.blocks,
            scenario.node_bodies
        );
        let applies = scenario
            .control
            .iter()
            .filter_map(|step| match &step.action {
                ControlAction::Grow { apply_when, .. }
                | ControlAction::Reset { apply_when, .. } => apply_when.as_ref(),
                _ => None,
            });
        let triggers = scenario
            .policy
            .holds
            .iter()
            .map(|hold| &hold.until)
            .chain(
                scenario
                    .control
                    .iter()
                    .filter_map(|step| step.when.as_ref()),
            )
            .chain(applies);
        if let Some(why) = triggers.filter_map(policy::undriven).next() {
            panic!("{why}");
        }
        let fixture = BlockFixture::regtest();
        let chain = fixture.uniform(fixture.anchor(), scenario.blocks, scenario.body_spec(), 0);
        let held = |bodies: u32| usize::try_from(bodies).expect("fixture chains are short");
        let node = Arc::new(EnginePort::new(fixture.headers()));
        preload(&node, &fixture, &chain, held(scenario.node_bodies));
        let replies = Arc::new(LateHandle::default());
        let service = Arc::new(ControlledBlockService::new(
            node.clone(),
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
        let live = LiveChain::new(fixture.clone(), chain.clone(), scenario.body_spec());
        Ports {
            fixture,
            chain,
            node,
            service,
            replies,
            mirrors,
            live,
            reads: MirrorReads::default(),
            probe: ProbeLog::default(),
            effects: Effects::default(),
            view: Rc::default(),
        }
    }

    /// Spawns the node on its controlled service under the scenario's
    /// policy, with its block sync tapped, then each mirror, links each
    /// mirror to the node once, then the probe if the scenario has one, and
    /// offers the scenario's control steps.
    fn build(sim: &mut Sim, scenario: &BlockScenario, ports: Ports) -> Self {
        let storage = PolicySource::new(
            sim,
            ports.service.clone(),
            scenario.policy.clone(),
            ports.effects.clone(),
        );
        let config = scenario.node_config();
        // Control steps grow and fork the chain with the scenario's body
        // spec, so no body the node can hold is larger than these.
        let largest_body = ports
            .chain
            .iter()
            .map(FixtureBlock::size)
            .chain([scenario.body_bytes])
            .max()
            .map_or(0, u64::from);
        let workers = storage_config(scenario).scheduler.workers;
        // The node holds its first `node_bodies` bodies committed, so its
        // verified tip starts at the last of them.
        let verified_tip = scenario
            .node_bodies
            .checked_sub(1)
            .and_then(|last| ports.chain.get(usize::try_from(last).ok()?))
            .map_or(0, |block| block.height.0);
        let limits = Limits::of(&config, workers, largest_body);
        // With one body per request, a revocation takes a request's only
        // height, so the routine retires the whole request. With more, a
        // request can lose some heights and keep the rest
        // (`gc_obsolete_outstanding`), a path no scenario reaches. The
        // ledger reconciliation and the bounds' `k` terms need a scenario
        // for it before anyone relies on them there.
        assert_eq!(
            limits.per_request, 1,
            "block-sync properties are checked only at one body per request"
        );
        ports.view.borrow_mut().owners = OwnerModel::new(limits, verified_tip);
        let view = ports.view.clone();
        view.borrow().semantic.borrow_mut().set_limit(&config);
        let (observer, mut tap) = BlockTap::new(sim, &ports.service, "node", view.clone());
        let wire = Wire::new(block_sync_serving_limit(&config));
        tap.label_with(wire.clone());
        let teardown = tap.teardown();
        if let Some(pause) = scenario.handoff_pause {
            sim.hold_tasks(Box::new(PausedHandoffs {
                handoffs: tap.handoffs(),
                view: view.clone(),
                from_height: pause.from_height,
                steps: pause.steps,
                first: Cell::new(None),
                over: Cell::new(false),
            }));
        }
        let storage = storage.with_tap(tap);
        let predicates = storage.predicates();
        sim.add_source(Box::new(storage));
        sim.add_source(Box::new(MirrorReadSource::new(
            ports.reads.clone(),
            predicates.clone(),
        )));
        let mut nodes = vec![BlockNode::spawn(
            sim,
            "node",
            1,
            &ports.node,
            NodeStorage::Controlled {
                service: ports.service,
                replies: ports.replies,
            },
            config,
            Some(observer),
        )];
        for ((index, engine), seed) in ports.mirrors.iter().enumerate().zip(2..) {
            let name = format!("mirror-{index}");
            nodes.push(BlockNode::spawn(
                sim,
                &name,
                seed,
                engine,
                NodeStorage::Mirror(
                    MirrorStorage::new(&name, engine.clone(), ports.reads.clone())
                        .withholding(&scenario.mirrors[index].withheld),
                ),
                ZakuraBlockSyncConfig::default(),
                None,
            ));
        }
        let topology = Rc::new(
            Topology::new(nodes, ports.effects, ports.live)
                .with_wire(wire)
                .with_tap(teardown),
        );
        for (index, spec) in (1..).zip(&scenario.mirrors) {
            match spec.dialer {
                Dialer::Mirror => topology.link(index, 0),
                Dialer::Node => topology.link(0, index),
            };
        }
        if scenario.probe {
            topology.link_probe(sim, ports.probe);
        }
        let control = Control::new(sim, topology.clone(), scenario.control.clone(), predicates);
        sim.add_source(Box::new(control));
        Storage {
            view,
            _topology: topology,
        }
    }

    /// [`storage_properties`], [`table_wiring_properties`], the resource
    /// properties, then the [semantic properties](semantic_properties).
    fn properties() -> Vec<Property<Self>> {
        let mut properties = storage_properties();
        properties.extend(table_wiring_properties());
        properties.extend(resource_properties());
        properties.extend(semantic_properties());
        properties
    }

    /// [`storage_sources`].
    fn sources() -> Sources {
        storage_sources()
    }

    /// The scenario the prefix's two bytes pick: [`STUCK_FLOOR_TAG`],
    /// [`PAUSED_HANDOFFS_TAG`], [`ABOVE_WINDOW_TAG`] and [`PEERS_LEAVE_TAG`]
    /// pick theirs, as do the semantic scenarios' tags
    /// ([`BlockScenario::semantic_drawn`]). All of them start with `ff`, so
    /// they come before the odd-first-byte arm. Otherwise an odd first byte
    /// picks the short budget, and an even one the default. Each tagged run is long or narrow, so
    /// fuzzing draws each on one prefix in 65,536.
    fn draw(prefix: &[u8]) -> BlockScenario {
        if let Some(scenario) = BlockScenario::semantic_drawn(prefix) {
            return scenario;
        }
        match prefix {
            [first, second, ..] if [*first, *second] == STUCK_FLOOR_TAG => {
                BlockScenario::stuck_floor()
            }
            [first, second, ..] if [*first, *second] == PAUSED_HANDOFFS_TAG => {
                BlockScenario::paused_handoffs()
            }
            [first, second, ..] if [*first, *second] == ABOVE_WINDOW_TAG => {
                BlockScenario::above_window()
            }
            [first, second, ..] if [*first, *second] == PEERS_LEAVE_TAG => {
                BlockScenario::peers_leave()
            }
            [first, ..] if first % 2 == 1 => BlockScenario::short_budget(),
            _ => BlockScenario::default(),
        }
    }
}

/// The scenario prefix that picks [`BlockScenario::stuck_floor`].
pub const STUCK_FLOOR_TAG: [u8; 2] = [0xFF, 0x02];

/// The scenario prefix that picks [`BlockScenario::paused_handoffs`].
pub const PAUSED_HANDOFFS_TAG: [u8; 2] = [0xFF, 0x03];

/// The scenario prefix that picks [`BlockScenario::above_window`].
pub const ABOVE_WINDOW_TAG: [u8; 2] = [0xFF, 0x04];

/// The scenario prefix that picks [`BlockScenario::peers_leave`].
pub const PEERS_LEAVE_TAG: [u8; 2] = [0xFF, 0x05];

/// [`HandoffPause`]'s hold: a routine task that recorded a handoff's start
/// and not its end, stopped in the budget's code, which only its release
/// reaches before the handoff ends.
struct PausedHandoffs {
    handoffs: TapHandoffs,
    view: Rc<RefCell<BlockSyncView>>,
    from_height: u32,
    steps: u64,
    /// The step after which the first pause began.
    first: Cell<Option<u64>>,
    /// Set once the pauses end.
    over: Cell<bool>,
}

impl TaskHold for PausedHandoffs {
    fn holds(&self, task: TaskId, at: &At) -> bool {
        if self.over.get() {
            return false;
        }
        let view = self.view.borrow();
        let step = view.step.unwrap_or(0);
        let past = view.owners.reserved() > view.owners.limits.reserved_bound(0);
        let expired = self
            .first
            .get()
            .is_some_and(|first| step >= first.saturating_add(self.steps));
        if past || expired {
            self.over.set(true);
            return false;
        }
        let releasing = at
            .site
            .is_some_and(|site| site.file().ends_with("transport/guard.rs"));
        let from = self.from_height;
        if !releasing || !self.handoffs.open(task).iter().any(|h| *h >= from) {
            return false;
        }
        if self.first.get().is_none() {
            self.first.set(Some(step));
        }
        true
    }
}

/// The semantic properties share the model in the node's view.
impl SemanticView for Rc<RefCell<BlockSyncView>> {
    fn semantic(&self) -> Rc<RefCell<SemanticModel>> {
        self.borrow().semantic.clone()
    }
}

/// The resource properties read the owner model in the node's view.
impl OwnerView for Rc<RefCell<BlockSyncView>> {
    fn read_owners<T>(&self, read: impl FnOnce(&OwnerModel) -> T) -> T {
        read(&self.borrow().owners)
    }
}

/// Live steps a run allows per fixture block. Each body crosses a link's
/// pipe tasks, whose atomics are each a step, so one takes a few hundred,
/// and twice that leaves room.
const STEPS_PER_BLOCK: u64 = 1_000;

/// The storage run's config for `scenario`: its workers, 2 by default, the default stack,
/// which engine planning needs, and the scenario's step budget, or room to
/// sync every block, including those its control steps grow or fork, and
/// then idle on timers.
pub fn storage_config(scenario: &BlockScenario) -> RunConfig {
    let changed: u64 = scenario
        .control
        .iter()
        .map(|step| match step.action {
            ControlAction::Grow { len, .. } => u64::from(len),
            ControlAction::Reset { fork, .. } => u64::from(fork.len),
            _ => 0,
        })
        .sum();
    RunConfig {
        scheduler: SchedulerConfig {
            workers: scenario.workers.unwrap_or(2),
            ..SchedulerConfig::default()
        },
        budget: scenario
            .steps
            .unwrap_or(8_000 + STEPS_PER_BLOCK * (u64::from(scenario.blocks) + changed)),
        drain_bound: 2_000,
        ..RunConfig::default()
    }
}

/// The block storage harness's properties: [`OPERATION_IDS`], active. A
/// test harness that wraps [`Storage`] registers them too.
pub fn storage_properties<S: HasView>() -> Vec<Property<S>> {
    vec![Property {
        id: PropertyId::new(OPERATION_IDS),
        claim: "the node's block operations get rising IDs, and each completes at most once, \
                after its submission",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::Storage],
        status: Status::Active,
        check: check_operation_ids::<S>,
        witness: None,
    }]
}

/// Stream 6's generated properties: every receiver property, the
/// downstream properties of the node's work, serving and output, and every
/// sender property. A test harness that wraps [`Storage`] registers them
/// too.
pub fn table_wiring_properties<S: HasView>() -> Vec<Property<S>> {
    let streams = block_sync_streams();
    let mut properties = table_properties(FAMILY, streams);
    properties.extend(downstream_properties(
        FAMILY,
        streams,
        &[Effect::Work, Effect::Serving, Effect::Output],
    ));
    properties.extend(sender_properties(FAMILY, streams));
    properties
}

/// What the harness's properties read: the node's storage facts, the
/// relays' frames, the node's closes and its view, plus the step loop's
/// task observations.
pub fn storage_sources() -> Sources {
    Sources::observed(&[
        Source::Schedule,
        Source::Storage,
        Source::Frames,
        Source::Closes,
        Source::View,
    ])
}

/// Checks [`OPERATION_IDS`] on the node's block storage.
fn check_operation_ids<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    check_service_operation_ids(BLOCK_SERVICE, cx)
}
