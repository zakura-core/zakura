//! `header-sync/release-wake`: `header-sync/storage` on the wake fixtures
//! that `header-sync/capacity-release-wakes` applies to ([`WakeScenario`]).
//! Its one scenario byte picks the fixture, and the rest of an input is
//! Plan bytes. It checks that property besides `header-sync/storage`'s,
//! since no other harness declares the fixture.

use std::rc::Rc;

use serde::{Deserialize, Serialize};
use zakura_scheduler::SimInstant;

use super::{
    storage_config, storage_properties, storage_sources, Dialer, MirrorSpec, Ports, Storage,
    StorageScenario, StorageView, Weights,
};
use crate::{
    domain::header_sync::{capacity_release_wakes, CACHED_PASS_REFILL},
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            wake::{WakeEvidence, WakeFixture},
            Chain, ControlAction, ControlStep, Fork, Policy, Topology,
        },
        Subsystem,
    },
    property::{EndViolation, HasView, Property, Sources},
    ActionSource, Boundary, Identity, ObservationLog, RunConfig, RunEnd, Sim,
};

/// The headers of room the fixture's body lag leaves: the node's verified
/// tip sits at genesis and its header tip [`NODE_TIP`] above, out of a
/// 4,000-header window.
pub const BODY_WINDOW: usize = 500;

/// The node's header tip in the fixture.
pub const NODE_TIP: u32 = 3_500;

/// How often [`WakeScenario::Deferred`]'s idle mirror relinks before the
/// waiting offer's `Status`. Each link change bumps the reactor's
/// scheduling key and buys a cached pass, so these spend the reactor's
/// 32-pass burst with room to spare.
pub const DEFERRING_RELINKS: u32 = 64;

/// [`WakeScenario::Deferred`]'s step budget, which its relinks need.
const DEFERRED_BUDGET: u64 = 30_000;

/// One of the wake fixtures. Every one rules out the reasons other than
/// room that an offer can wait, as [`WakeFixture`] describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WakeScenario {
    /// `mirror-0` and `mirror-1` sit 400 headers past the node's tip on
    /// their own forks, and their rounds use the [`BODY_WINDOW`] with
    /// their prepares held. `mirror-2` starts at the node's tip, then grows
    /// 300 headers and relinks once both prepares are submitted, so its
    /// one `Status` arrives with no room left. Once its offer waits for
    /// room, `mirror-0`'s prepare is released to fail with a local fault,
    /// so its handler retires the round and its leases return. A prepare's
    /// fault is a failure, not a resource stall, so discovery never
    /// pauses. `mirror-1`'s prepare stays held until the wake's deadline
    /// has passed, then runs ([`Settle`]).
    Release,
    /// [`WakeScenario::Release`], with a fourth mirror at the node's tip
    /// that relinks [`DEFERRING_RELINKS`] times before `mirror-2` grows.
    /// Virtual time stays at zero until then, so no pass refills, and the
    /// cached pass the release wants is deferred to the budget's refill.
    Deferred,
}

impl WakeScenario {
    /// The storage scenario this fixture runs.
    pub fn storage(self) -> StorageScenario {
        let mut scenario = release();
        if self == WakeScenario::Deferred {
            scenario.mirrors.push(MirrorSpec {
                chain: Chain::main(NODE_TIP),
                dialer: Dialer::Mirror,
                max_headers_per_response: None,
            });
            // Control steps run in order, so these come before
            // `mirror-2`'s growth. `linked` counts the setup link too.
            let relinks = (1..=DEFERRING_RELINKS).map(|count| ControlStep {
                peer: "mirror-3".to_owned(),
                action: ControlAction::Relink,
                when: Some(Trigger::Count(
                    count,
                    Box::new(Trigger::Linked("mirror-3".to_owned())),
                )),
            });
            scenario.controls.splice(0..0, relinks);
        }
        scenario
    }

    /// The live steps a run may take. The fixture's 800 filling headers
    /// and the waiting mirror's 300 take more than the storage default,
    /// and the deferred fixture's relinks more again.
    fn budget(self) -> u64 {
        match self {
            WakeScenario::Release => 20_000,
            WakeScenario::Deferred => DEFERRED_BUDGET,
        }
    }
}

/// [`WakeScenario::Release`]'s storage scenario.
fn release() -> StorageScenario {
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
        max_headers_per_response: None,
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
                max_headers_per_response: None,
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
        weights: Weights::default(),
    }
}

/// The policy hold [`Settle`] opens: `mirror-1`'s, the second of
/// [`release`]'s holds.
const MIRROR_1_HOLD: usize = 1;

/// Lets `mirror-1`'s held prepare run once the clock is past every opened
/// wake's deadline, so a run can end with nothing held.
///
/// `header-sync/capacity-release-wakes` has concluded each wake's
/// obligation by then, answered or not. An undeferred one expires the
/// first time no task can run, and the clock moves only then. A deferred
/// one expires once the clock passes its deadline, which is never more
/// than [`CACHED_PASS_REFILL`] after the wake opened. So the release comes
/// after the measured boundary, including when a bypass stopped the wake,
/// and a run is the same as one without it up to that boundary. It offers
/// no time boundary of its own, since one could change when the clock
/// moves before that.
struct Settle {
    wake: WakeEvidence,
    topology: Rc<Topology>,
    /// Set once the release was picked.
    done: bool,
}

impl Settle {
    /// Whether a wake opened and the clock at `now` is past the deadline
    /// of the last one.
    fn due(&self, now: SimInstant) -> bool {
        self.wake
            .last_opened()
            .and_then(|at| at.checked_add(CACHED_PASS_REFILL))
            .is_some_and(|deadline| now > deadline)
    }
}

impl ActionSource for Settle {
    /// Offers `peer:node:settle` once, while [`Settle::due`].
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if !self.done && self.due(now) {
            out.push((Identity::peer("node", "settle"), 0));
        }
    }

    /// None, as [`Settle`] says.
    fn boundaries(&self, _now: SimInstant, _out: &mut Vec<Boundary>) {}

    /// Opens `mirror-1`'s hold.
    fn execute(&mut self, _local: u64, _log: &mut ObservationLog) {
        self.done = true;
        self.topology.release(MIRROR_1_HOLD);
    }
}

/// `header-sync/storage` on a [`WakeScenario`], with the wake fixture
/// declared.
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
    const SCENARIO_BYTES: usize = 1;
    type Scenario = WakeScenario;
    type Prepared = Ports;

    /// [`storage_config`], with the fixture's step budget.
    fn config(scenario: &WakeScenario) -> RunConfig {
        RunConfig {
            budget: scenario.budget(),
            ..storage_config()
        }
    }

    /// The storage harness's ports for the fixture, with the fixture
    /// declared.
    fn prepare(scenario: &WakeScenario) -> Ports {
        Ports {
            wake: WakeEvidence::new(Some(WakeFixture {
                body_window: BODY_WINDOW,
            })),
            ..Storage::prepare(&scenario.storage())
        }
    }

    /// The storage harness on the fixture, with its [`Settle`] source.
    fn build(sim: &mut Sim, scenario: &WakeScenario, ports: Ports) -> Self {
        let wake = ports.wake.clone();
        let storage = Storage::build(sim, &scenario.storage(), ports);
        sim.add_source(Box::new(Settle {
            wake,
            topology: storage.topology.clone(),
            done: false,
        }));
        ReleaseWake(storage)
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

    /// Bit 0 of the scenario byte: 0 [`WakeScenario::Release`], 1
    /// [`WakeScenario::Deferred`].
    fn draw(prefix: &[u8]) -> WakeScenario {
        if prefix.first().is_some_and(|byte| byte & 1 == 1) {
            WakeScenario::Deferred
        } else {
            WakeScenario::Release
        }
    }
}
