//! Header-sync harnesses. `header-sync/smoke` runs the reactor on the step
//! loop with inert storage and no peers. `header-sync/storage` runs a node
//! on controlled storage with mirror peers (M4).

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
        preload, Chain, Control, ControlStep, DispatchFlag, Effects, HeaderNode, Policy,
        PolicySource, Topology,
    },
    Subsystem,
};
use crate::{
    observe::StorageFact,
    property::{CheckCtx, HasView, Kind, Property, PropertyId, Source, Sources, Status, Verdict},
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
    /// The nodes and links. They live until the run ends, so no reactor sees
    /// its inputs close.
    _topology: Rc<Topology>,
}

/// What [`Storage`]'s checks read between steps.
pub struct StorageView {
    /// The node's port, for its pending operations and its engine's state.
    pub port: Arc<ControlledPort>,
    /// Whether any reactor dispatched a storage call externally.
    pub direct_dispatch_broken: bool,
}

impl HasView for Storage {
    type View = StorageView;

    /// The node's port and the dispatch guard's flag.
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

    /// Spawns the node on its controlled port under the scenario's policy,
    /// then each mirror, links each mirror to the node once, and adds the
    /// `Control` source.
    fn build(sim: &mut Sim, scenario: &StorageScenario, ports: Ports) -> Self {
        let dispatch = DispatchFlag::default();
        let effects = Effects::default();
        let storage = PolicySource::new(
            sim,
            ports.node.clone(),
            scenario.policy.clone(),
            effects.clone(),
        );
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

/// The storage harness's properties, [`OPERATION_IDS`] then
/// [`DIRECT_DISPATCH`], both active. A test harness that wraps
/// [`Storage`] with its view registers them too.
pub fn storage_properties<S: HasView<View = StorageView>>() -> Vec<Property<S>> {
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
/// harness's view, plus the step loop's task observations.
pub fn storage_sources() -> Sources {
    Sources::observed(&[Source::Schedule, Source::Storage, Source::View])
}

/// A submission or completion on the node's storage, instance 0.
fn operation_fact(obs: &Observation) -> Option<&StorageFact> {
    match obs {
        Observation::Storage {
            instance: 0,
            fact: fact @ (StorageFact::Submitted { .. } | StorageFact::Completed { .. }),
            ..
        } => Some(fact),
        _ => None,
    }
}

/// Checks [`OPERATION_IDS`] at each step that logs a submission or
/// completion on the node's storage. A check keeps no state between steps,
/// so it replays the node's facts from the run's start, and reports the
/// first finding among this step's.
fn check_operation_ids<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    if !cx.observations().any(|obs| operation_fact(obs).is_some()) {
        return Verdict::NotApplicable;
    }
    let mut ids = OperationIds::default();
    for entry in cx.log.entries() {
        let Some(fact) = operation_fact(&entry.obs) else {
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
fn check_direct_dispatch<S: HasView<View = StorageView>>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    if cx.view().direct_dispatch_broken {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::{StorageRequest, StorageResult};

    /// A submission of operation `id`.
    fn submitted(id: u64) -> StorageFact {
        StorageFact::Submitted {
            id,
            request: StorageRequest::ContinuationLocator,
            digest: String::new(),
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
}
