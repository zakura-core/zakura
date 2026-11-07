//! `header-sync/flood`: `header-sync/storage` on the flood fixtures
//! ([`FloodScenario`]), where a node-side commit publishes a new committed
//! state while 16 mirrors flood one of the reactor's event queues. Its one
//! scenario byte picks the fixture, and the rest of an input is Plan bytes.
//! It checks the watch properties besides `header-sync/storage`'s.

use serde::{Deserialize, Serialize};
use zakura_network::zakura::observe::EventQueue;

use super::{
    storage_config, storage_properties, storage_sources, Dialer, MirrorSpec, Ports, Storage,
    StorageScenario, StorageView,
};
use crate::{
    domain::header_sync::{
        watch_end_violations, watch_observed_under_flood, watch_observed_under_lifecycle_flood,
    },
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            Chain, ControlAction, ControlStep, Policy,
        },
        Subsystem,
    },
    property::{EndViolation, HasView, Property, Sources},
    ObservationLog, RunConfig, RunEnd, Sim,
};

/// The node's header tip in the fixtures.
const NODE_TIP: u32 = 4;

/// The mirrors that flood or churn. 16 keep a queue ready in most turns
/// (`tests/header_flood.rs`).
pub const FLOODERS: usize = 16;

/// The headers a fixture's commit adds.
const COMMIT: u32 = 2;

/// The live steps a run may take. 16 flooding or churning mirrors take
/// many steps a turn.
const BUDGET: u64 = 20_000;

/// One of the flood fixtures.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FloodScenario {
    /// 16 mirrors at the node's tip flood its wire queue. A 17th, one
    /// header ahead, gets a locator read held. Once every flood session is
    /// admitted, the read is released, and as it completes the node commits
    /// two headers past its tip, so the read's completion refreshes the
    /// committed snapshot right after the publication.
    Wire,
    /// 16 mirrors at the node's tip churn its lifecycle queue. Once every
    /// churn has started, the node commits two headers past its tip. Each
    /// later connect refreshes the committed snapshot.
    Lifecycle,
}

impl FloodScenario {
    /// The event queue this fixture floods.
    pub fn queue(self) -> EventQueue {
        match self {
            FloodScenario::Wire => EventQueue::Wire,
            FloodScenario::Lifecycle => EventQueue::Lifecycle,
        }
    }

    /// The storage scenario this fixture runs.
    pub fn storage(self) -> StorageScenario {
        let at_tip = || MirrorSpec {
            chain: Chain::main(NODE_TIP),
            dialer: Dialer::Mirror,
        };
        let step = |peer: String, action| ControlStep {
            peer,
            action,
            when: None,
        };
        let action = match self {
            FloodScenario::Wire => ControlAction::Flood,
            FloodScenario::Lifecycle => ControlAction::Churn,
        };
        let mut mirrors: Vec<MirrorSpec> = (0..FLOODERS).map(|_| at_tip()).collect();
        let mut controls: Vec<ControlStep> = (0..FLOODERS)
            .map(|index| step(format!("mirror-{index}"), action))
            .collect();
        let commit = |when| ControlStep {
            peer: "node".to_owned(),
            action: ControlAction::Commit { len: COMMIT },
            when,
        };
        let mut holds = Vec::new();
        match self {
            FloodScenario::Wire => {
                mirrors.push(MirrorSpec {
                    chain: Chain::main(NODE_TIP + 1),
                    dialer: Dialer::Mirror,
                });
                let locator = Select::nth(OpKind::Locator, 1);
                holds.push(Hold {
                    select: locator.clone(),
                    until: Trigger::Released,
                    then: vec![Allow::Engine],
                    deadline_ms: None,
                });
                // The held read is released once the node has admitted
                // every flood session, its setup link's and its flood's, so
                // their lifecycle events are mostly behind it. The commit
                // comes as the read completes, so its queued reply, which
                // the run loop takes before its events, refreshes the
                // committed snapshot right after the publication.
                controls.push(ControlStep {
                    peer: "node".to_owned(),
                    action: ControlAction::Release { hold: 0 },
                    when: Some(Trigger::All(
                        (0..FLOODERS)
                            .map(|index| {
                                Trigger::Count(
                                    2,
                                    Box::new(Trigger::SessionAdmitted(format!("mirror-{index}"))),
                                )
                            })
                            .collect(),
                    )),
                });
                controls.push(commit(Some(Trigger::Completed(locator))));
            }
            FloodScenario::Lifecycle => controls.push(commit(None)),
        }
        StorageScenario {
            node: Chain::main(NODE_TIP),
            mirrors,
            policy: Policy {
                holds,
                ..Policy::converging()
            },
            controls,
        }
    }
}

/// `header-sync/storage` on a [`FloodScenario`].
pub struct Flood(Storage, FloodScenario);

impl HasView for Flood {
    type View = StorageView;

    /// The storage harness's view.
    fn view(&self) -> StorageView {
        self.0.view()
    }

    /// The storage harness's end.
    fn ended(&self, end: &RunEnd) {
        self.0.ended(end);
    }

    /// The storage harness's end violations, then the flooded queue's watch
    /// property's ([`watch_end_violations`]).
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        let mut found = self.0.end_violations(log);
        found.extend(watch_end_violations(
            &self.0.adoption,
            self.1.queue(),
            self.0.topology.shutdown_picked(),
        ));
        found
    }

    /// The storage harness's stamped facts.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl Subsystem for Flood {
    const NAME: &'static str = "header-sync/flood";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 1;
    type Scenario = FloodScenario;
    type Prepared = Ports;

    /// [`storage_config`], with the fixtures' step budget.
    fn config(_: &FloodScenario) -> RunConfig {
        RunConfig {
            budget: BUDGET,
            ..storage_config()
        }
    }

    /// The storage harness's ports for the fixture.
    fn prepare(scenario: &FloodScenario) -> Ports {
        Storage::prepare(&scenario.storage())
    }

    /// The storage harness on the fixture.
    fn build(sim: &mut Sim, scenario: &FloodScenario, ports: Ports) -> Self {
        Flood(Storage::build(sim, &scenario.storage(), ports), *scenario)
    }

    /// The storage harness's properties, then the two watch properties,
    /// which apply only where a commit publishes during a flood.
    fn properties() -> Vec<Property<Self>> {
        let mut properties = storage_properties();
        properties.push(watch_observed_under_flood());
        properties.push(watch_observed_under_lifecycle_flood());
        properties
    }

    /// The storage harness's sources.
    fn sources() -> Sources {
        storage_sources()
    }

    /// Bit 0 of the scenario byte: 0 [`FloodScenario::Wire`], 1
    /// [`FloodScenario::Lifecycle`].
    fn draw(prefix: &[u8]) -> FloodScenario {
        if prefix.first().is_some_and(|byte| byte & 1 == 1) {
            FloodScenario::Lifecycle
        } else {
            FloodScenario::Wire
        }
    }
}
