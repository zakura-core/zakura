//! `header-sync/flood`: `header-sync/storage` on the flood fixtures
//! ([`FloodScenario`]), where a node-side commit publishes a new committed
//! state while 16 mirrors flood one of the reactor's event queues. Its one
//! scenario byte picks the fixture, and the rest of an input is Plan bytes.
//! It checks the watch properties besides `header-sync/storage`'s.

use serde::{Deserialize, Serialize};
use zakura_network::zakura::observe::EventQueue;

use super::{
    storage_config, storage_properties, storage_sources, Dialer, MirrorSpec, Ports, Storage,
    StorageScenario, StorageView, Weights,
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
            max_headers_per_response: None,
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
                    max_headers_per_response: None,
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
            weights: Weights::default(),
        }
    }
}

/// `header-sync/storage` on a [`FloodScenario`].
pub struct Flood(Storage, FloodScenario);

/// The storage harness's properties, then the two watch properties, which
/// apply only where a commit publishes during a flood.
fn flood_properties<S: HasView<View = StorageView>>() -> Vec<Property<S>> {
    let mut properties = storage_properties();
    properties.push(watch_observed_under_flood());
    properties.push(watch_observed_under_lifecycle_flood());
    properties
}

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

    /// The storage harness's end violations, then each queue's watch
    /// property's ([`watch_end_violations`]), the flooded queue's first. A
    /// loop that wedges with a publication unread leaves it unread under
    /// both queues' rules, so both report it.
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        let mut found = self.0.end_violations(log);
        let flooded = self.1.queue();
        let other = match flooded {
            EventQueue::Wire => EventQueue::Lifecycle,
            EventQueue::Lifecycle => EventQueue::Wire,
        };
        for queue in [flooded, other] {
            found.extend(watch_end_violations(
                &self.0.adoption,
                queue,
                self.stopping(),
            ));
        }
        found
    }

    /// Whether a `Shutdown` step was picked, the stop that exempts an
    /// unread publication from [`watch_end_violations`].
    fn stopping(&self) -> bool {
        self.0.topology.shutdown_picked()
    }

    /// The storage harness's stamped facts.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }

    /// The storage harness's store snapshot.
    fn snapshot(&self) -> Option<serde_json::Value> {
        self.0.snapshot()
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

    /// [`flood_properties`].
    fn properties() -> Vec<Property<Self>> {
        flood_properties()
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

#[cfg(test)]
mod tests {
    use zakura_network::zakura::observe::EventQueue;

    use super::{flood_properties, Flood, FloodScenario};
    use crate::{
        check::FindingSubject,
        domain::header_sync::{WATCH_OBSERVED_UNDER_FLOOD, WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD},
        harness::{
            entry,
            header_node::harness_commit,
            header_sync::{Ports, StorageView},
            scripted::{findings, recorded},
            Subsystem,
        },
        property::{EndViolation, HasView, Property, Sources},
        ObservationLog, RunConfig, RunEnd, Sim,
    };

    /// The flood harness, committing twice more as its run ends, after the
    /// reactor's last turn, so its loop reads neither state: a loop wedged
    /// with two publications unread. Everything else, its end findings
    /// above all, is [`Flood`]'s own.
    struct TwoUnread(Flood);

    impl HasView for TwoUnread {
        type View = StorageView;

        /// The flood harness's view.
        fn view(&self) -> StorageView {
            self.0.view()
        }

        /// Commits two states through the harness's commit, then hands the
        /// end to the flood harness.
        fn ended(&self, end: &RunEnd) {
            let storage = &self.0 .0;
            let fixture = self.0 .1.storage().weights.fixture();
            for _ in 0..2 {
                harness_commit(&storage.port, &fixture, &storage.adoption, 1)
                    .expect("the node's chain extends by a fixture header");
            }
            self.0.ended(end);
        }

        /// [`Flood::end_violations`].
        fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
            self.0.end_violations(log)
        }

        /// The flood harness's stop.
        fn stopping(&self) -> bool {
            self.0.stopping()
        }

        /// The flood harness's stamped facts.
        fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
            self.0.stamps_after(after)
        }

        /// The flood harness's store snapshot.
        fn snapshot(&self) -> Option<serde_json::Value> {
            self.0.snapshot()
        }
    }

    impl Subsystem for TwoUnread {
        const NAME: &'static str = "test/flood-two-unread-at-a-settled-end";
        const GENERATOR: u32 = Flood::GENERATOR;
        const SCENARIO_BYTES: usize = Flood::SCENARIO_BYTES;
        type Scenario = FloodScenario;
        type Prepared = Ports;

        /// [`Flood::config`].
        fn config(scenario: &FloodScenario) -> RunConfig {
            Flood::config(scenario)
        }

        /// [`Flood::prepare`].
        fn prepare(scenario: &FloodScenario) -> Ports {
            Flood::prepare(scenario)
        }

        /// The flood harness on the fixture.
        fn build(sim: &mut Sim, scenario: &FloodScenario, ports: Ports) -> Self {
            TwoUnread(Flood::build(sim, scenario, ports))
        }

        /// [`flood_properties`], as the flood harness checks them.
        fn properties() -> Vec<Property<Self>> {
            flood_properties()
        }

        /// [`Flood::sources`].
        fn sources() -> Sources {
            Flood::sources()
        }

        /// [`Flood::draw`].
        fn draw(prefix: &[u8]) -> FloodScenario {
            Flood::draw(prefix)
        }
    }

    /// C21 through the flood harness's own end path: on each fixture, two
    /// states published and both unread when the run settles are two end
    /// findings of each watch rule, each naming its own publication, in the
    /// full report recorded beside the trace and its fresh replay. A
    /// [`Flood::end_violations`] that kept only the first would fail it,
    /// which the adoption sidecar test can't show, as it judges the rules
    /// through its own end path.
    #[test]
    fn the_flood_end_path_reports_every_unread_publication() {
        for scenario in [FloodScenario::Wire, FloodScenario::Lifecycle] {
            let scenario_json = serde_json::to_value(scenario).expect("a fixture serializes");
            let full = recorded(&entry::<TwoUnread>(), &scenario_json, &[]);
            for (property, queue) in [
                (WATCH_OBSERVED_UNDER_FLOOD, EventQueue::Wire),
                (WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD, EventQueue::Lifecycle),
            ] {
                let found = findings(&full, property);
                let unread: Vec<String> = found
                    .violations
                    .iter()
                    .filter(|violation| violation.actual.contains("published and unread"))
                    .map(|violation| match &violation.subject {
                        Some(FindingSubject::Publication { state_version }) => {
                            state_version.clone()
                        }
                        other => panic!("{property}: an end finding names {other:?}"),
                    })
                    .collect();
                assert_eq!(unread.len(), 2, "{scenario:?} {property}: {found:?}");
                assert_ne!(unread[0], unread[1], "{scenario:?} {property}");
                assert!(
                    found
                        .violations
                        .iter()
                        .all(|violation| violation.actual.contains(&format!("{queue:?} events"))),
                    "{scenario:?} {property}: {found:?}"
                );
                assert!(
                    matches!(found.outcome, crate::PropertyOutcome::Violated { .. }),
                    "{scenario:?} {property}: {:?}",
                    found.outcome
                );
            }
        }
    }
}
