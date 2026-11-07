//! `header-sync/release-wake`: `header-sync/storage` on one fixed scenario,
//! the wake fixture that `header-sync/capacity-release-wakes` applies to
//! ([`StorageScenario::release_wake`]). Its inputs are Plan bytes alone.
//! It checks that property besides `header-sync/storage`'s, since no other
//! harness declares the fixture.

use super::{
    storage_config, storage_properties, storage_sources, Dialer, MirrorSpec, Ports, Storage,
    StorageScenario, StorageView,
};
use crate::{
    domain::header_sync::capacity_release_wakes,
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            wake::{WakeEvidence, WakeFixture},
            Chain, ControlAction, ControlStep, Fork, Policy,
        },
        Subsystem,
    },
    property::{EndViolation, HasView, Property, Sources},
    ObservationLog, RunConfig, RunEnd, Sim,
};

/// The headers of room the fixture's body lag leaves: the node's verified
/// tip sits at genesis and its header tip [`NODE_TIP`] above, out of a
/// 4,000-header window.
pub const BODY_WINDOW: usize = 500;

/// The node's header tip in the fixture.
pub const NODE_TIP: u32 = 3_500;

/// The live steps a run may take. The fixture's 800 filling headers and
/// the waiting mirror's 300 take more steps than the storage default.
const BUDGET: u64 = 20_000;

impl StorageScenario {
    /// The wake fixture. `mirror-0` and `mirror-1` sit 400 headers past
    /// the node's tip on their own forks, and their rounds use the
    /// [`BODY_WINDOW`] with their prepares held. `mirror-2` starts at the
    /// node's tip, then grows 300 headers and relinks once both prepares
    /// are submitted, so its one `Status` arrives with no room left. Once
    /// its offer waits for room, `mirror-0`'s prepare is released to fail
    /// with a local fault, so its handler retires the round and its leases
    /// return. `mirror-1`'s prepare stays held.
    pub fn release_wake() -> Self {
        let prepare = |nth| Select::nth(OpKind::Prepare, nth);
        let held = |nth, then| Hold {
            select: prepare(nth),
            until: Trigger::Released,
            then: vec![then],
            deadline_ms: None,
        };
        let filling = |branch| MirrorSpec {
            chain: Chain {
                main: NODE_TIP,
                fork: Some(Fork { branch, len: 400 }),
            },
            dialer: Dialer::Mirror,
        };
        let control = |peer: &str, action, when| ControlStep {
            peer: peer.to_owned(),
            action,
            when: Some(when),
        };
        StorageScenario {
            node: Chain::main(NODE_TIP),
            mirrors: vec![
                filling(1),
                filling(2),
                MirrorSpec {
                    chain: Chain::main(NODE_TIP),
                    dialer: Dialer::Mirror,
                },
            ],
            policy: Policy {
                holds: vec![held(1, Allow::LocalResource), held(2, Allow::Engine)],
                ..Policy::converging()
            },
            controls: vec![
                control(
                    "mirror-2",
                    ControlAction::Grow { len: 300 },
                    Trigger::Submitted(prepare(2)),
                ),
                control(
                    "mirror-2",
                    ControlAction::Relink,
                    Trigger::Grown("mirror-2".to_owned()),
                ),
                control(
                    "node",
                    ControlAction::Release { hold: 0 },
                    Trigger::HeadroomWait("mirror-2".to_owned()),
                ),
            ],
        }
    }
}

/// `header-sync/storage` on [`StorageScenario::release_wake`], with the
/// wake fixture declared.
pub struct ReleaseWake(Storage);

impl HasView for ReleaseWake {
    type View = StorageView;

    /// The storage harness's view.
    fn view(&self) -> StorageView {
        self.0.view()
    }

    /// The storage harness's end.
    fn ended(&self, end: &RunEnd) {
        self.0.ended(end);
    }

    /// The storage harness's end violations.
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        self.0.end_violations(log)
    }

    /// The storage harness's wake facts' stamps.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl Subsystem for ReleaseWake {
    const NAME: &'static str = "header-sync/release-wake";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = StorageScenario;
    type Prepared = Ports;

    /// [`storage_config`], with a larger step budget.
    fn config(_: &StorageScenario) -> RunConfig {
        RunConfig {
            budget: BUDGET,
            ..storage_config()
        }
    }

    /// The storage harness's ports, with the wake fixture declared.
    fn prepare(scenario: &StorageScenario) -> Ports {
        Ports {
            wake: WakeEvidence::new(Some(WakeFixture {
                body_window: BODY_WINDOW,
            })),
            ..Storage::prepare(scenario)
        }
    }

    /// The storage harness.
    fn build(sim: &mut Sim, scenario: &StorageScenario, ports: Ports) -> Self {
        ReleaseWake(Storage::build(sim, scenario, ports))
    }

    /// The storage harness's properties, then
    /// `header-sync/capacity-release-wakes`, which applies only here.
    fn properties() -> Vec<Property<Self>> {
        let mut properties = storage_properties();
        properties.push(capacity_release_wakes());
        properties
    }

    /// The storage harness's sources.
    fn sources() -> Sources {
        storage_sources()
    }

    /// The only scenario.
    fn draw(_: &[u8]) -> StorageScenario {
        StorageScenario::release_wake()
    }
}
