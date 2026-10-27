//! Header-sync harnesses. `header-sync/smoke` runs the reactor on the step
//! loop with inert storage and no peers. `header-sync/storage` runs a node
//! on controlled storage with mirror peers (M4), with the node's reactor
//! tapped.

use std::{any::Any, collections::BTreeSet, rc::Rc, sync::Arc};

use serde::{Deserialize, Serialize};
use zakura_chain::{block::Height, parameters::Network};
use zakura_network::zakura::{
    spawn_header_sync_reactor, FullStateFrontiers, HeaderSyncStartup, ZakuraHeaderSyncConfig,
    LOCAL_MAX_MESSAGE_BYTES,
};
use zakura_node_services::header_chain::InertHeaderChainPort;
use zakura_scheduler::SchedulerConfig;

use zakura_storage::header::{
    adapter::EnginePort, controlled::ControlledPort, fixture::HeaderFixture,
};

use super::{
    header_node::{
        preload, tap::ReactorTap, Chain, Control, ControlStep, DispatchFlag, Effects, HeaderNode,
        Policy, PolicySource, Topology,
    },
    Subsystem,
};
use crate::{
    check::StepCtx,
    observe::StorageFact,
    property::{HasView, Status as PropertyStatus},
    trace::meta::PropertyRef,
    Checker, NoChecks, Observation, ObservationLog, PropertyOutcome, PropertyReport, RunConfig,
    RunEnd, Sim, Violation,
};

/// The smoke run's scenario. It has no knobs yet.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {}

/// The header-sync reactor at Regtest genesis, with inert storage and no
/// peers. It checks nothing, so a run shows only that the reactor runs on a
/// `Plan` and shuts down.
pub struct Smoke {
    /// The reactor's handle and action channel. They live until the run
    /// ends, so the reactor doesn't see its inputs close.
    _inputs: Box<dyn Any>,
}

impl HasView for Smoke {
    type View = ();

    /// Nothing, since the run checks nothing.
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

    /// Checks nothing.
    fn checker(_: &Rc<Self>, _: &Scenario) -> Box<dyn Checker> {
        Box::new(NoChecks)
    }

    /// None.
    fn properties(_: &Scenario) -> Vec<PropertyRef> {
        Vec::new()
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

/// A mirror peer: real header-sync code on an engine port.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MirrorSpec {
    /// Its name, such as `mira`.
    pub name: String,
    /// The chain it holds when the run starts.
    pub chain: Chain,
    /// Which end of its link to the node dials.
    pub dialer: Dialer,
}

/// The `header-sync/storage` scenario.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageScenario {
    /// The chain the node under test, Nora, holds when the run starts.
    pub node: Chain,
    /// The mirror peers, each linked once to the node at setup.
    pub mirrors: Vec<MirrorSpec>,
    /// How the run completes Nora's storage operations.
    pub policy: Policy,
    /// Link actions the `Plan` may take, in order.
    #[serde(default)]
    pub controls: Vec<ControlStep>,
}

impl Default for StorageScenario {
    /// Nora at genesis and Mira at A5, under the converging policy.
    fn default() -> Self {
        StorageScenario {
            node: Chain::default(),
            mirrors: vec![MirrorSpec {
                name: "mira".to_owned(),
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
    /// Nora's controlled port.
    pub node: Arc<ControlledPort>,
    /// Each mirror's engine port, in scenario order.
    pub mirrors: Vec<Arc<EnginePort>>,
}

/// Nora on a controlled port with mirror peers, checked by [`NodeChecks`].
pub struct Storage {
    port: Arc<ControlledPort>,
    dispatch: DispatchFlag,
    /// The nodes and links. They live until the run ends, so no reactor sees
    /// its inputs close.
    _topology: Rc<Topology>,
}

/// What [`Storage`]'s checks read between steps.
pub struct StorageView {
    /// Nora's port, for its pending operations and its engine's state.
    pub port: Arc<ControlledPort>,
    /// Whether any reactor dispatched a storage call externally.
    pub direct_dispatch_broken: bool,
}

impl HasView for Storage {
    type View = StorageView;

    /// Nora's port and the dispatch guard's flag.
    fn view(&self) -> StorageView {
        StorageView {
            port: self.port.clone(),
            direct_dispatch_broken: self.dispatch.get(),
        }
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
    fn prepare(scenario: &StorageScenario) -> Ports {
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

    /// Spawns Nora on her controlled port under the scenario's policy, with
    /// her reactor tapped, then each mirror, links each mirror to Nora once,
    /// and adds the `Control` source. Only Nora is tapped.
    fn build(sim: &mut Sim, scenario: &StorageScenario, ports: Ports) -> Self {
        let dispatch = DispatchFlag::default();
        let effects = Effects::default();
        let storage = PolicySource::new(
            sim,
            ports.node.clone(),
            scenario.policy.clone(),
            effects.clone(),
        );
        let (observer, tap) = ReactorTap::new(sim, &ports.node, "nora");
        let storage = storage.with_tap(tap);
        let predicates = storage.predicates();
        sim.add_source(Box::new(storage));
        let mut nodes = vec![HeaderNode::spawn(
            sim,
            "nora",
            1,
            ports.node.clone(),
            &ports.fixture,
            scenario.node.tip(&ports.fixture),
            dispatch.clone(),
            Some(observer),
        )];
        for ((spec, port), seed) in scenario.mirrors.iter().zip(&ports.mirrors).zip(2..) {
            nodes.push(HeaderNode::spawn(
                sim,
                &spec.name,
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
        Storage {
            port: ports.node,
            dispatch,
            _topology: topology,
        }
    }

    /// [`NodeChecks`] on the guard's flag.
    fn checker(this: &Rc<Self>, _: &StorageScenario) -> Box<dyn Checker> {
        Box::new(NodeChecks::new(this.dispatch.clone()))
    }

    /// [`OPERATION_IDS`] and [`DIRECT_DISPATCH`], active.
    fn properties(_: &StorageScenario) -> Vec<PropertyRef> {
        [OPERATION_IDS, DIRECT_DISPATCH]
            .into_iter()
            .map(|id| PropertyRef {
                id: id.to_owned(),
                status: PropertyStatus::Active,
            })
            .collect()
    }

    /// The default scenario.
    fn draw(_: &[u8]) -> StorageScenario {
        StorageScenario::default()
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

/// Checks [`OPERATION_IDS`] on Nora's storage facts, instance 0, after
/// every step, and reports [`DIRECT_DISPATCH`] on the step the guard's
/// flag is set.
pub struct NodeChecks {
    dispatch: DispatchFlag,
    /// The last submitted ID.
    submitted: u64,
    /// Every completed ID.
    completed: BTreeSet<u64>,
    /// Steps that delivered a submission or completion.
    applicable: u32,
}

impl NodeChecks {
    /// Checks that read `dispatch`.
    pub fn new(dispatch: DispatchFlag) -> Self {
        NodeChecks {
            dispatch,
            submitted: 0,
            completed: BTreeSet::new(),
            applicable: 0,
        }
    }

    /// What's wrong with `fact`, if anything.
    fn check(&mut self, fact: &StorageFact) -> Option<(String, String)> {
        match *fact {
            StorageFact::Submitted { id, .. } => {
                let expected = self.submitted + 1;
                self.submitted = id;
                (id != expected).then(|| {
                    (
                        format!("operation {expected} submitted next"),
                        format!("operation {id} submitted"),
                    )
                })
            }
            StorageFact::Completed { id, .. } => {
                if id > self.submitted {
                    Some((
                        format!(
                            "a completion of a submitted operation, at most {}",
                            self.submitted
                        ),
                        format!("operation {id} completed before its submission"),
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

impl Checker for NodeChecks {
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        if self.dispatch.get() {
            return Some(Violation {
                property: DIRECT_DISPATCH.to_owned(),
                step: cx.step,
                witness: vec![cx.step],
                expected: "every storage call goes to the node's port".to_owned(),
                actual: "a reactor sent a call to its action channel".to_owned(),
            });
        }
        let mut applied = false;
        for obs in cx.observations() {
            if let Observation::Storage {
                instance: 0, fact, ..
            } = obs
            {
                applied |= matches!(
                    fact,
                    StorageFact::Submitted { .. } | StorageFact::Completed { .. }
                );
                if let Some((expected, actual)) = self.check(fact) {
                    return Some(Violation {
                        property: OPERATION_IDS.to_owned(),
                        step: cx.step,
                        witness: vec![cx.step],
                        expected,
                        actual,
                    });
                }
            }
        }
        self.applicable += u32::from(applied);
        None
    }

    /// Each property's outcome: violated if the run ended on it, else held
    /// where it applied.
    fn at_end(&mut self, end: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        [(OPERATION_IDS, self.applicable), (DIRECT_DISPATCH, 1)]
            .into_iter()
            .map(|(property, applicable)| {
                let outcome = match &end.violation {
                    Some(violation) if violation.property == property => {
                        PropertyOutcome::Violated {
                            witness: violation.witness.clone(),
                        }
                    }
                    _ if applicable > 0 => PropertyOutcome::Held { applicable },
                    _ => PropertyOutcome::NotApplicable,
                };
                PropertyReport {
                    property: property.to_owned(),
                    outcome,
                    holds_since: None,
                }
            })
            .collect()
    }
}
