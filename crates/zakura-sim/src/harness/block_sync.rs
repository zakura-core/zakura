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

use std::{cell::RefCell, rc::Rc, sync::Arc};

use serde::{Deserialize, Serialize};
use zakura_scheduler::SchedulerConfig;
use zakura_storage::{
    block::{
        controlled::ControlledBlockService,
        fixture::{BlockFixture, BodySpec, FixtureBlock},
    },
    header::adapter::EnginePort,
};

use super::{
    block_node::{
        control::{Control, ControlAction, ControlStep},
        default_limits, policy, preload,
        probe::ProbeLog,
        tap::BlockTap,
        view::BlockSyncView,
        BlockNode, Effects, LateHandle, LiveChain, MirrorReadSource, MirrorReads, MirrorStorage,
        NodeStorage, Policy, PolicySource, Topology,
    },
    header_sync::{check_service_operation_ids, Dialer},
    Subsystem,
};
use crate::{
    property::{
        Capture, CheckCtx, HasView, Kind, Property, PropertyId, Source, Sources, Status, Verdict,
    },
    storage::BLOCK_SERVICE,
    RunConfig, Sim,
};

/// The node's block operations get rising IDs, each completes at most once,
/// and only after its submission.
pub const OPERATION_IDS: &str = "block-sync/storage-operation-ids";

/// A mirror peer: real block-sync code on full state that holds the whole
/// fixture chain. It's named `mirror-<i>` by its index in
/// [`BlockScenario::mirrors`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MirrorSpec {
    /// Which end of its link to the node dials.
    pub dialer: Dialer,
}

/// The `block-sync/storage` scenario.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockScenario {
    /// Blocks in the fixture chain above genesis. Every actor holds their
    /// headers.
    pub blocks: u32,
    /// Each body's serialized size in bytes, which its header advertises
    /// exactly.
    pub body_bytes: u32,
    /// The bodies the node holds when the run starts, from height 1.
    #[serde(default)]
    pub node_bodies: u32,
    /// The mirror peers, each linked once to the node at setup.
    pub mirrors: Vec<MirrorSpec>,
    /// Whether a scripted probe links to the node at setup, after the
    /// mirrors. Only `Control` steps make it send anything.
    #[serde(default)]
    pub probe: bool,
    /// How the run completes the node's block operations and the mirrors'
    /// range reads.
    pub policy: Policy,
    /// Scripted harness actions, offered in order.
    #[serde(default)]
    pub control: Vec<ControlStep>,
}

impl Default for BlockScenario {
    /// Eight 2,000-byte blocks the node lacks, from one mirror that dials
    /// it, under the converging policy.
    fn default() -> Self {
        BlockScenario {
            blocks: 8,
            body_bytes: 2_000,
            node_bodies: 0,
            mirrors: vec![MirrorSpec {
                dialer: Dialer::Mirror,
            }],
            probe: false,
            policy: Policy::converging(),
            control: Vec::new(),
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
/// mirror peers, checked by [`storage_properties`]. Only the node's block
/// sync is tapped.
pub struct Storage {
    /// The node's owner state, as its tap folds it.
    view: Rc<RefCell<BlockSyncView>>,
    /// The nodes and links. They live until the run ends, so no reactor sees
    /// its inputs close.
    _topology: Rc<Topology>,
}

impl HasView for Storage {
    type View = BlockSyncView;

    /// The node's owner state as of the tap's last observation.
    fn view(&self) -> BlockSyncView {
        self.view.borrow().clone()
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
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
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
        let triggers = scenario.policy.holds.iter().map(|hold| &hold.until).chain(
            scenario
                .control
                .iter()
                .filter_map(|step| step.when.as_ref()),
        );
        if let Some(why) = triggers.filter_map(policy::undriven).next() {
            panic!("{why}");
        }
        let fixture = BlockFixture::regtest();
        let chain = fixture.uniform(
            fixture.anchor(),
            scenario.blocks,
            BodySpec::exact(scenario.body_bytes),
            0,
        );
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
        let live = LiveChain::new(
            fixture.clone(),
            chain.clone(),
            BodySpec::exact(scenario.body_bytes),
        );
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
    /// policy, with its block sync tapped, then each mirror, links each mirror to the node once, then
    /// the probe if the scenario has one, and offers the scenario's control
    /// steps.
    fn build(sim: &mut Sim, scenario: &BlockScenario, ports: Ports) -> Self {
        let storage = PolicySource::new(
            sim,
            ports.service.clone(),
            scenario.policy.clone(),
            ports.effects.clone(),
        );
        let view = ports.view.clone();
        let (observer, tap) = BlockTap::new(sim, &ports.service, "node", view.clone());
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
            Some(observer),
        )];
        for ((index, engine), seed) in ports.mirrors.iter().enumerate().zip(2..) {
            let name = format!("mirror-{index}");
            nodes.push(BlockNode::spawn(
                sim,
                &name,
                seed,
                engine,
                NodeStorage::Mirror(MirrorStorage::new(
                    &name,
                    engine.clone(),
                    ports.reads.clone(),
                )),
                None,
            ));
        }
        let topology = Rc::new(Topology::new(nodes, ports.effects, ports.live));
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

    /// [`storage_properties`].
    fn properties() -> Vec<Property<Self>> {
        storage_properties()
    }

    /// [`storage_sources`].
    fn sources() -> Sources {
        storage_sources()
    }

    /// The default scenario.
    fn draw(_: &[u8]) -> BlockScenario {
        BlockScenario::default()
    }
}

/// Live steps a run allows per fixture block. Each body crosses a link's
/// pipe tasks, whose atomics are each a step, so one takes a few hundred,
/// and twice that leaves room.
const STEPS_PER_BLOCK: u64 = 1_000;

/// The storage run's config for `scenario`: 2 workers, the default stack,
/// which engine planning needs, and room to sync every block, including
/// those its control steps grow or fork, and then idle on timers.
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
            workers: 2,
            ..SchedulerConfig::default()
        },
        budget: 8_000 + STEPS_PER_BLOCK * (u64::from(scenario.blocks) + changed),
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

/// What [`storage_properties`] read: the node's storage facts, plus the step
/// loop's task observations.
pub fn storage_sources() -> Sources {
    Sources::observed(&[Source::Schedule, Source::Storage])
}

/// Checks [`OPERATION_IDS`] on the node's block storage.
fn check_operation_ids<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    check_service_operation_ids(BLOCK_SERVICE, cx)
}
