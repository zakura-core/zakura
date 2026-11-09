//! `header-sync/liveness`: `header-sync/storage` on hdr-10's liveness
//! fixtures ([`LivenessScenario`]). Its one scenario byte picks the
//! fixture, and the rest of an input is Plan bytes. It checks
//! `header-sync/hold-release-wakes` besides `header-sync/storage`'s
//! properties, since only it arms the node's hold-wake evidence.
//!
//! Its fixtures declare `BoundedAdversary` (Rule 13 of the hdr-9 to hdr-13
//! plan). Every peer is an honest mirror or a scripted control, so the
//! assumption holds after every step, and the declaration only turns on the
//! step loop's livelock limit at a tenth of the run's budget
//! ([`crate::harness::run_config`]). A reactor that spins at one instant
//! with an obligation open then violates `sim/no-livelock`, instead of
//! keeping the clock from the quiescence that would expire the obligation
//! and ending the run at its step budget. `header-sync/storage` itself
//! declares nothing: its honest runs take up to its whole 4,000-step budget
//! at one instant, so the limit would fail them. So this harness runs only
//! its named fixtures, never an arbitrary scenario, and its budget,
//! [`LIVENESS_BUDGET`], puts the limit at more than twice each fixture's
//! honest peak, which a test measures.
//!
//! Every fixture ends: once each held reply has been handled, or the clock
//! has moved past it, which is when `header-sync/hold-release-wakes` has
//! concluded its obligation either way, a [`Finish`] step stops every node,
//! so the run ends idle rather than at its budget (Rule 13).

use std::rc::Rc;

use serde::{Deserialize, Serialize};
use serde_json::json;
use zakura_rt::sync::watch;
use zakura_scheduler::SimInstant;

use super::{storage_config, storage_properties, storage_sources, Ports, Storage, StorageView};
use crate::{
    domain::header_sync::hold_release_wakes,
    harness::{
        header_node::{
            hold_wake::{HoldWakeEvidence, HoldWakeFact},
            policy::{Allow, Hold, OpKind, ReplyHold, Select, Trigger},
            Chain, ControlAction, ControlStep, Policy, Topology,
        },
        header_sync::{Dialer, MirrorSpec, StorageScenario},
        Subsystem,
    },
    property::{Assume, AssumeRecord, Assumptions, EndViolation, HasView, Property, Sources},
    ActionSource, Boundary, Identity, ObservationLog, RunConfig, RunEnd, Sim,
};

/// The live steps a liveness run may take. The livelock limit is a tenth of
/// it, 4,000 steps at one instant. The fixtures' honest runs take at most
/// about 1,870 there, the serving ones, measured over 16 plans each, and
/// a test keeps every fixture under half the limit.
pub const LIVENESS_BUDGET: u64 = 40_000;

/// A storage operation kind a hold fixture holds: the reactor's three.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HeldKind {
    /// The first locator read.
    Locator,
    /// The first prepare.
    Prepare,
    /// The first apply.
    Apply,
}

impl HeldKind {
    /// Every kind, in order.
    pub const ALL: [HeldKind; 3] = [HeldKind::Locator, HeldKind::Prepare, HeldKind::Apply];

    /// The storage operation kind.
    pub fn op(self) -> OpKind {
        match self {
            HeldKind::Locator => OpKind::Locator,
            HeldKind::Prepare => OpKind::Prepare,
            HeldKind::Apply => OpKind::Apply,
        }
    }
}

/// One of the liveness fixtures. Each is a fixed storage scenario
/// ([`LivenessScenario::storage`]) that leaves the reactor one thing to wait
/// for (Rule 8).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LivenessScenario {
    /// The node at A3 syncing from `mirror-0` at A6, its only peer. The
    /// node's first operation of the kind is held until a `Release` step
    /// opens it, which the Plan may take any time once that operation is
    /// submitted. The mirror is ahead of the node, so the node serves it
    /// nothing, and once the held reply is delivered nothing else is left to
    /// wake the reactor before the clock moves.
    Held(HeldKind),
    /// [`LivenessScenario::Held`], with the node also serving `mirror-1` at
    /// A1, and the release offered only once a page read for `mirror-1`
    /// has completed too, an unrelated completion, so the reply can land
    /// while the reactor is busy with other peers.
    HeldWhileServing(HeldKind),
    /// [`LivenessScenario::Held`] for the first apply, whose hold opens by
    /// its own trigger, two virtual seconds after its submission, rather
    /// than by a step.
    OpenedByTrigger,
    /// [`LivenessScenario::Held`] for the first apply, with its operation
    /// unheld and its reply held instead: it commits, and its reply reaches
    /// the reactor two virtual seconds later.
    ReplyHeld,
    /// [`LivenessScenario::Held`] for the first prepare, with a `Shutdown`
    /// step the Plan may take once that prepare completes, so under some
    /// plans the node begins stopping before its reactor handles the
    /// delivered reply.
    ShutdownAfterRelease,
}

impl LivenessScenario {
    /// Every fixture, in the order the scenario byte picks them.
    pub fn all() -> Vec<LivenessScenario> {
        let mut all: Vec<LivenessScenario> = HeldKind::ALL
            .into_iter()
            .map(LivenessScenario::Held)
            .collect();
        all.extend(
            HeldKind::ALL
                .into_iter()
                .map(LivenessScenario::HeldWhileServing),
        );
        all.extend([
            LivenessScenario::OpenedByTrigger,
            LivenessScenario::ReplyHeld,
            LivenessScenario::ShutdownAfterRelease,
        ]);
        all
    }

    /// The storage scenario this fixture runs.
    pub fn storage(self) -> StorageScenario {
        match self {
            LivenessScenario::Held(kind) => held(kind.op()),
            LivenessScenario::HeldWhileServing(kind) => {
                let mut scenario = held(kind.op());
                scenario.mirrors.push(MirrorSpec {
                    chain: Chain::main(1),
                    dialer: Dialer::Mirror,
                });
                scenario.controls[0].when = Some(Trigger::All(vec![
                    Trigger::Submitted(Select::nth(kind.op(), 1)),
                    Trigger::Completed(Select {
                        kind: Some(OpKind::Read),
                        ..Select::default()
                    }),
                ]));
                scenario
            }
            LivenessScenario::OpenedByTrigger => {
                let mut scenario = held(OpKind::Apply);
                scenario.policy.holds[0].until = Trigger::VirtualTime(2_000);
                scenario.controls.clear();
                scenario
            }
            LivenessScenario::ReplyHeld => {
                let mut scenario = held(OpKind::Apply);
                scenario.policy.holds.clear();
                scenario.policy.reply_holds = vec![ReplyHold {
                    select: Select::nth(OpKind::Apply, 1),
                    until: Trigger::VirtualTime(2_000),
                }];
                scenario.controls.clear();
                scenario
            }
            LivenessScenario::ShutdownAfterRelease => {
                let mut scenario = held(OpKind::Prepare);
                scenario.controls.push(ControlStep {
                    peer: "node".to_owned(),
                    action: ControlAction::Shutdown,
                    when: Some(Trigger::Completed(Select::nth(OpKind::Prepare, 1))),
                });
                scenario
            }
        }
    }
}

/// [`LivenessScenario::Held`]'s storage scenario, for `kind`.
fn held(kind: OpKind) -> StorageScenario {
    StorageScenario {
        node: Chain::main(3),
        mirrors: vec![MirrorSpec {
            chain: Chain::main(6),
            dialer: Dialer::Mirror,
        }],
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(kind, 1),
                until: Trigger::Released,
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        controls: vec![ControlStep {
            peer: "node".to_owned(),
            action: ControlAction::Release { hold: 0 },
            when: Some(Trigger::Submitted(Select::nth(kind, 1))),
        }],
        weights: Default::default(),
    }
}

/// Ends a liveness run once its obligations have concluded: at least one
/// held reply has reached the reactor, and each has been handled or the
/// clock has moved past it, and the fixture's own `Shutdown` step, if it
/// has one, was picked. Its pick begins the node under test's stop, as a
/// `Shutdown` step's does, and its task then stops every node
/// ([`Topology::finish`]). It offers no time boundary of its own, so it
/// never changes when the clock moves before then.
struct Finish {
    hold_wake: HoldWakeEvidence,
    topology: Rc<Topology>,
    /// Whether the fixture has its own `Shutdown` step, which must come
    /// first.
    waits_for_shutdown: bool,
    /// Wakes the task that carries the finish out.
    to_task: watch::Sender<bool>,
    /// Set once the finish was picked.
    done: bool,
}

impl Finish {
    /// Whether the run's obligations have concluded at `now`, as
    /// [`Finish`] says.
    fn due(&self, now: SimInstant) -> bool {
        if self.done || (self.waits_for_shutdown && !self.topology.shutdown_picked()) {
            return false;
        }
        let facts = self.hold_wake.facts();
        let concluded = |call: u64, at: SimInstant| {
            now > at
                || facts.iter().any(|(_, fact)| {
                    matches!(fact, HoldWakeFact::Handled { call: handled, .. } if *handled == call)
                })
        };
        let opened: Vec<(u64, SimInstant)> = facts
            .iter()
            .filter_map(|(_, fact)| match fact {
                HoldWakeFact::Opened { call, at, .. } => Some((*call, *at)),
                HoldWakeFact::Handled { .. } => None,
            })
            .collect();
        !opened.is_empty() && opened.into_iter().all(|(call, at)| concluded(call, at))
    }
}

impl ActionSource for Finish {
    /// Offers `peer:node:finish` once, while [`Finish::due`].
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if self.due(now) {
            out.push((Identity::peer("node", "finish"), 0));
        }
    }

    /// None, as [`Finish`] says.
    fn boundaries(&self, _now: SimInstant, _out: &mut Vec<Boundary>) {}

    /// Begins the stop and wakes the task.
    fn execute(&mut self, _local: u64, _log: &mut ObservationLog) {
        self.done = true;
        self.topology.begin_finish();
        self.to_task.send_replace(true);
    }
}

/// What every liveness fixture declares: `BoundedAdversary`, with no
/// hostile peer, as the module doc says. A test harness that wraps
/// [`Liveness`] declares it too.
pub fn assumptions<S>() -> Assumptions<S> {
    Assumptions::declare(vec![AssumeRecord {
        assume: Assume::BoundedAdversary,
        params: json!({ "hostile_peers": 0 }),
    }])
}

/// `header-sync/storage` on a liveness fixture, with the hold-wake
/// evidence armed and `BoundedAdversary` declared.
pub struct Liveness(Storage);

impl HasView for Liveness {
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

    /// The storage harness's stamps.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl Subsystem for Liveness {
    const NAME: &'static str = "header-sync/liveness";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 1;
    type Scenario = LivenessScenario;
    type Prepared = Ports;

    /// [`storage_config`], with [`LIVENESS_BUDGET`].
    fn config(_: &LivenessScenario) -> RunConfig {
        RunConfig {
            budget: LIVENESS_BUDGET,
            ..storage_config()
        }
    }

    /// The storage harness's ports for the fixture, with the hold-wake
    /// evidence armed.
    fn prepare(scenario: &LivenessScenario) -> Ports {
        Ports {
            hold_wake: HoldWakeEvidence::armed(),
            ..Storage::prepare(&scenario.storage())
        }
    }

    /// The storage harness on the fixture, with its [`Finish`] source and
    /// the task that carries the finish out.
    fn build(sim: &mut Sim, scenario: &LivenessScenario, ports: Ports) -> Self {
        let hold_wake = ports.hold_wake.clone();
        let storage = scenario.storage();
        let storage_harness = Storage::build(sim, &storage, ports);
        let topology = storage_harness.topology.clone();
        let (to_task, mut finishing) = watch::channel(false);
        let task_topology = topology.clone();
        sim.scheduler().spawn_root("finish", async move {
            while !*finishing.borrow_and_update() {
                if finishing.changed().await.is_err() {
                    return;
                }
            }
            task_topology.finish();
        });
        sim.add_source(Box::new(Finish {
            hold_wake,
            topology,
            waits_for_shutdown: storage
                .controls
                .iter()
                .any(|step| step.action == ControlAction::Shutdown),
            to_task,
            done: false,
        }));
        Liveness(storage_harness)
    }

    /// The storage harness's properties, then
    /// `header-sync/hold-release-wakes`, which applies only here.
    fn properties() -> Vec<Property<Self>> {
        let mut properties = storage_properties();
        properties.push(hold_release_wakes());
        properties
    }

    /// The storage harness's sources.
    fn sources() -> Sources {
        storage_sources()
    }

    /// [`assumptions`].
    fn assumptions(_: &LivenessScenario) -> Assumptions<Self> {
        assumptions()
    }

    /// The fixture [`LivenessScenario::all`] lists at the scenario byte's
    /// value, modulo their count.
    fn draw(prefix: &[u8]) -> LivenessScenario {
        let all = LivenessScenario::all();
        let byte = usize::from(prefix.first().copied().unwrap_or(0));
        all[byte % all.len()]
    }
}
