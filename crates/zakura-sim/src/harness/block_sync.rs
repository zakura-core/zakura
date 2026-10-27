//! The `block-sync/storage` harness: a block-sync node on controlled block
//! storage, with mirror peers that hold the whole fixture chain.
//!
//! The node's real reactor, routines and sequencer run against a
//! [`ControlledBlockService`] that shares its engine with the node's header
//! side, as zakurad's driver shares zakura-state with header sync. Its
//! serving reads through the same service, but no mirror asks it for a
//! body. Header sync itself doesn't run: every actor's headers are
//! committed before the run, so block sync's work comes only from the
//! committed views.
//!
//! The scenario leaves out what the service doesn't model: zakurad's legacy
//! handoff and `SyncCoordinator` phases, the debug throughput probe, stall
//! traces, the fail-closed shutdown when an invalid-body record isn't
//! durable, the 30 s timeouts on state calls, and the tampered-body
//! disconnect. No block property covers them.

use std::{rc::Rc, sync::Arc};

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
        default_limits, preload, BlockNode, LateHandle, MirrorStorage, NodeStorage, Policy,
        PolicySource, Topology,
    },
    header_sync::{check_service_operation_ids, Dialer},
    Subsystem,
};
use crate::{
    property::{CheckCtx, HasView, Kind, Property, PropertyId, Source, Sources, Status, Verdict},
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
    /// How the run completes the node's block operations.
    pub policy: Policy,
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
            policy: Policy::converging(),
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
}

/// The node under test, named `node`, on a controlled block service with
/// mirror peers, checked by [`storage_properties`].
pub struct Storage {
    /// The nodes and links. They live until the run ends, so no reactor sees
    /// its inputs close.
    _topology: Rc<Topology>,
}

impl HasView for Storage {
    type View = ();

    /// Nothing, since no property reads it.
    fn view(&self) {}
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
    /// If the node holds more bodies than the scenario has blocks.
    fn prepare(scenario: &BlockScenario) -> Ports {
        assert!(
            scenario.node_bodies <= scenario.blocks,
            "a node holds at most the scenario's {} blocks, not {}",
            scenario.blocks,
            scenario.node_bodies
        );
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
        Ports {
            fixture,
            chain,
            node,
            service,
            replies,
            mirrors,
        }
    }

    /// Spawns the node on its controlled service under the scenario's
    /// policy, then each mirror, and links each mirror to the node once.
    fn build(sim: &mut Sim, scenario: &BlockScenario, ports: Ports) -> Self {
        let storage = PolicySource::new(sim, ports.service.clone(), scenario.policy);
        sim.add_source(Box::new(storage));
        let mut nodes = vec![BlockNode::spawn(
            sim,
            "node",
            1,
            &ports.node,
            NodeStorage::Controlled {
                service: ports.service,
                replies: ports.replies,
            },
        )];
        for ((index, engine), seed) in ports.mirrors.iter().enumerate().zip(2..) {
            nodes.push(BlockNode::spawn(
                sim,
                &format!("mirror-{index}"),
                seed,
                engine,
                NodeStorage::Mirror(MirrorStorage::new(engine.clone())),
            ));
        }
        let topology = Rc::new(Topology::new(nodes));
        for (index, spec) in (1..).zip(&scenario.mirrors) {
            match spec.dialer {
                Dialer::Mirror => topology.link(index, 0),
                Dialer::Node => topology.link(0, index),
            };
        }
        Storage {
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
/// which engine planning needs, and room to sync every block and then idle
/// on timers.
pub fn storage_config(scenario: &BlockScenario) -> RunConfig {
    RunConfig {
        scheduler: SchedulerConfig {
            workers: 2,
            ..SchedulerConfig::default()
        },
        budget: 8_000 + STEPS_PER_BLOCK * u64::from(scenario.blocks),
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
