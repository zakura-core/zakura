//! Scripted harness actions on a block-sync run's links and chain.
//!
//! [`Control`] offers a scenario's [`ControlStep`]s in order, each once, so
//! the `Plan` decides when each happens, once its trigger holds. Harness
//! tasks carry out each link, probe and mirror action on their next step,
//! since those take locks that tasks hold across scheduling points, where
//! only a task can wait. So a pick only queues the action, and triggers
//! count the effect. Each kind of work has its own task, so a probe send
//! the node stops reading, or a mirror busy serving, delays no close or
//! relink.
//!
//! A chain change reaches the node under test in two steps of their own,
//! one preparing its headers and one applying them, since the step loop
//! alone reads the node's engine. A body commit can land between them, and
//! leaves the preparation valid. A recorded consensus-invalid body can land
//! there too, and moves the header generation, so the node prepares the
//! change again, as header sync would. No other actor changes the node's
//! headers, so any other stale apply fails the run. The change reaches the
//! mirrors before the node, or after it, as the action says. Chain changes
//! run one stage at a time, in script order. An action can hold the node's
//! apply step until a trigger holds, and a hold can wait for
//! [`Trigger::Prepared`](crate::harness::policy::Trigger::Prepared), so a scenario
//! can land an operation between the two steps on every schedule.
//!
//! A teardown runs on the link task, since it closes links. The two bypass
//! witnesses run in the step loop, since they read the node's engine and
//! its block service's owner state, as completions do.

use std::{cell::RefCell, collections::VecDeque, rc::Rc};

use serde::{Deserialize, Serialize};
use zakura_rt::sync::mpsc;
use zakura_scheduler::SimInstant;

use super::{
    policy::{Predicates, Trigger},
    probe::ProbeFrame,
    ChainChange, NodeTarget, Topology, PROBE,
};
use crate::{harness::policy::LinkEffect, ActionSource, Boundary, Identity, ObservationLog, Sim};

/// A fork of the chain: `len` blocks on fixture branch `branch`, extending
/// the chain's block at height `at`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fork {
    /// The fixture branch, which keeps the fork's blocks apart from every
    /// other branch's.
    pub branch: u8,
    /// The height the fork extends, below the chain's tip.
    pub at: u32,
    /// Its blocks. The fork must end above the chain's tip.
    pub len: u32,
}

/// A harness action.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ControlAction {
    /// Close the newest link to `peer`.
    Close {
        /// A mirror's name, or `probe`.
        peer: String,
    },
    /// Link `peer` again, with a new connection and session, in its newest
    /// link's direction. The probe links only once.
    Relink {
        /// A mirror's name.
        peer: String,
    },
    /// Extend the chain by `len` blocks from its tip on every actor: their
    /// headers on each engine, and their bodies in each mirror's full state.
    /// The node under test must download the bodies.
    Grow {
        /// The new blocks.
        len: u32,
        /// Whether the node takes the headers before the mirrors take the
        /// change, so its headers run ahead of its peers' bodies. The
        /// mirrors go first by default.
        #[serde(default)]
        node_first: bool,
        /// If set, the node applies the prepared headers only once this
        /// holds. Applied at once by default.
        #[serde(default)]
        apply_when: Option<Trigger>,
    },
    /// Switch every actor to `fork`: its headers on each engine, which
    /// reorganizes the node's selected chain through its real engine, and
    /// its bodies in each mirror's full state. The node's verified chain
    /// resets once it commits the fork's bodies over its own.
    Reset {
        /// The fork.
        fork: Fork,
        /// Whether the node takes the headers before the mirrors take the
        /// change. The mirrors go first by default.
        #[serde(default)]
        node_first: bool,
        /// If set, the node applies the prepared headers only once this
        /// holds. Applied at once by default.
        #[serde(default)]
        apply_when: Option<Trigger>,
    },
    /// Send `frames` from the probe, in order. The scenario must link one.
    ProbeSend {
        /// The frames.
        frames: Vec<ProbeFrame>,
    },
    /// Tear the node under test down: close every link, cancel its reactor,
    /// and drop every handle the harness holds, so its block sync ends and
    /// its sequencer releases what it holds. Its tap expects the observer
    /// to go once it logs the sequencer's close. A later relink fails.
    Teardown,
    /// A bypass witness for direct work insertion: answer the node's oldest
    /// pending needed-blocks query from its engine, straight through its
    /// handle, so block sync's work queue takes work that its block service
    /// never answered.
    InsertWork,
    /// A bypass witness for an alternate storage dispatch: send block sync a
    /// committed verdict for the node's oldest pending submission, straight
    /// through its handle, so the sequencer finishes a submission that its
    /// block service never committed.
    DispatchVerdict,
    /// A bypass witness for an alternate submission callback: hand the
    /// node's block service a `SubmitBlock` straight, for the body of its
    /// oldest pending submission, under an owner that no request of the
    /// node's names.
    SubmitUnrequested,
}

impl ControlAction {
    /// Who the action's identity names: a peer, or `chain` for a change to
    /// every actor's chain.
    fn actor(&self) -> &str {
        match self {
            ControlAction::Close { peer } | ControlAction::Relink { peer } => peer,
            ControlAction::Grow { .. } | ControlAction::Reset { .. } => "chain",
            ControlAction::ProbeSend { .. } => PROBE,
            ControlAction::Teardown
            | ControlAction::InsertWork
            | ControlAction::DispatchVerdict
            | ControlAction::SubmitUnrequested => "node",
        }
    }

    /// The action's name in its identity.
    fn name(&self) -> &'static str {
        match self {
            ControlAction::Close { .. } => "close",
            ControlAction::Relink { .. } => "relink",
            ControlAction::Grow { .. } => "grow",
            ControlAction::Reset { .. } => "reset",
            ControlAction::ProbeSend { .. } => "send",
            ControlAction::Teardown => "teardown",
            ControlAction::InsertWork => "insert-work",
            ControlAction::DispatchVerdict => "dispatch-verdict",
            ControlAction::SubmitUnrequested => "submit-unrequested",
        }
    }
}

/// One scripted [`ControlAction`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlStep {
    /// What to do.
    pub action: ControlAction,
    /// Offered only once this holds. Always offered without one.
    #[serde(default)]
    pub when: Option<Trigger>,
}

/// One stage of a chain change. Stages run one at a time, in order.
enum Stage {
    /// The mirrors make the change, on the chain task.
    Mirrors(ChainChange),
    /// The node prepares the change's headers, as a step of its own.
    NodePrepare(ChainChange, Gate),
    /// The node applies the prepared headers, as a step of its own, once
    /// its gate holds.
    NodeApply(ChainChange, NodeTarget, Gate),
}

/// What a chain change's node apply step waits for, if anything: its
/// action's `apply_when`.
type Gate = Option<Trigger>;

/// The chain changes' stages not yet done, in order, shared with the chain
/// task.
type Stages = Rc<RefCell<VecDeque<Stage>>>;

/// The token of the step that runs a chain change's node stage.
const CHANGE_NODE: u64 = u64::MAX;

/// Offers a scenario's scripted actions in order, each once, as
/// `peer:<actor>:<action>`, and each node stage of a chain change as
/// `peer:node:<action>` to prepare it and `peer:node:<action>-apply` to
/// apply it. See the module doc.
pub struct Control {
    script: Vec<ControlStep>,
    topology: Rc<Topology>,
    predicates: Rc<RefCell<Predicates>>,
    /// The next step to offer.
    next: usize,
    stages: Stages,
    /// Closes, relinks and teardowns, for the link task.
    to_links: mpsc::Sender<ControlAction>,
    /// Probe sends, for the probe task.
    to_probe: mpsc::Sender<Vec<ProbeFrame>>,
    /// Wakes the chain task when a mirror stage comes first.
    to_chain: mpsc::Sender<()>,
}

impl Control {
    /// A source for `script` on `topology`, gated by `predicates`, with its
    /// tasks spawned on `sim`.
    pub fn new(
        sim: &mut Sim,
        topology: Rc<Topology>,
        script: Vec<ControlStep>,
        predicates: Rc<RefCell<Predicates>>,
    ) -> Self {
        // Each channel holds the whole script.
        let (to_links, mut links) = mpsc::channel::<ControlAction>(script.len().max(1));
        let (to_probe, mut probe) = mpsc::channel::<Vec<ProbeFrame>>(script.len().max(1));
        let (to_chain, mut chain) = mpsc::channel::<()>(1);
        let stages = Stages::default();
        let task_topology = topology.clone();
        sim.scheduler().spawn_root("control", async move {
            while let Some(action) = links.recv().await {
                match action {
                    ControlAction::Close { peer } => task_topology
                        .link_to(&peer)
                        .unwrap_or_else(|| panic!("the scenario links `{peer}`"))
                        .close(),
                    ControlAction::Relink { peer } => task_topology.relink(&peer),
                    ControlAction::Teardown => task_topology.teardown(),
                    action => unreachable!("the link task gets only links, not {action:?}"),
                }
            }
        });
        // Only scripts that use them get the other tasks, so a script
        // without them schedules as before.
        if script
            .iter()
            .any(|step| matches!(step.action, ControlAction::ProbeSend { .. }))
        {
            let task_topology = topology.clone();
            sim.scheduler().spawn_root("control-probe", async move {
                while let Some(frames) = probe.recv().await {
                    task_topology.probe_send(&frames).await;
                }
            });
        }
        if script.iter().any(|step| {
            matches!(
                step.action,
                ControlAction::Grow { .. } | ControlAction::Reset { .. }
            )
        }) {
            let task_topology = topology.clone();
            let task_stages = stages.clone();
            sim.scheduler().spawn_root("control-chain", async move {
                while chain.recv().await.is_some() {
                    // The step loop never takes a mirror stage, so the front
                    // stays this one while the mirrors change.
                    loop {
                        let change = match task_stages.borrow().front() {
                            Some(Stage::Mirrors(change)) => change.clone(),
                            _ => break,
                        };
                        task_topology.change_mirrors(&change).await;
                        task_stages.borrow_mut().pop_front();
                    }
                }
            });
        }
        Control {
            script,
            topology,
            predicates,
            next: 0,
            stages,
            to_links,
            to_probe,
            to_chain,
        }
    }

    /// Wakes the chain task if a mirror stage comes first. A wake already
    /// queued covers this one.
    fn wake_chain(&self) {
        if matches!(self.stages.borrow().front(), Some(Stage::Mirrors(_))) {
            let _ = self.to_chain.try_send(());
        }
    }
}

impl ActionSource for Control {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        let action = |change: &ChainChange| match change.effect {
            LinkEffect::Reset => "reset",
            _ => "grow",
        };
        match self.stages.borrow().front() {
            Some(Stage::NodePrepare(change, _)) => {
                out.push((Identity::peer("node", action(change)), CHANGE_NODE));
            }
            Some(Stage::NodeApply(change, _, gate))
                if gate
                    .as_ref()
                    .is_none_or(|when| self.predicates.borrow().control_holds(when, now)) =>
            {
                out.push((
                    Identity::peer("node", &format!("{}-apply", action(change))),
                    CHANGE_NODE,
                ));
            }
            Some(Stage::NodeApply(..) | Stage::Mirrors(_)) | None => {}
        }
        let Some(step) = self.script.get(self.next) else {
            return;
        };
        let open = step
            .when
            .as_ref()
            .is_none_or(|when| self.predicates.borrow().control_holds(when, now));
        if open {
            // A script is far shorter than u64.
            out.push((
                Identity::peer(step.action.actor(), step.action.name()),
                self.next as u64,
            ));
        }
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        if let Some(Stage::NodeApply(_, _, Some(when))) = self.stages.borrow().front() {
            self.predicates.borrow().control_boundaries(when, now, out);
        }
        if let Some(when) = self
            .script
            .get(self.next)
            .and_then(|step| step.when.as_ref())
        {
            self.predicates.borrow().control_boundaries(when, now, out);
        }
    }

    fn execute(&mut self, token: u64, _: &mut ObservationLog) {
        if token == CHANGE_NODE {
            let stage = self.stages.borrow_mut().pop_front();
            let next = match stage {
                Some(Stage::NodePrepare(change, gate)) => self
                    .topology
                    .prepare_node(&change)
                    .map(|target| Stage::NodeApply(change, target, gate)),
                Some(Stage::NodeApply(change, target, gate)) => {
                    if self.topology.apply_node(&change, target) {
                        None
                    } else {
                        Some(Stage::NodePrepare(change, gate))
                    }
                }
                _ => unreachable!("`enabled` offers only a node stage that comes first"),
            };
            if let Some(next) = next {
                self.stages.borrow_mut().push_front(next);
            }
            self.wake_chain();
            return;
        }
        let action = self.script[self.next].action.clone();
        self.next += 1;
        let full = "each channel holds the whole script, and its task outlives the run";
        match action {
            ControlAction::Grow {
                node_first,
                ref apply_when,
                ..
            }
            | ControlAction::Reset {
                node_first,
                ref apply_when,
                ..
            } => {
                let Some(change) = self.topology.plan_change(&action) else {
                    return;
                };
                let node = Stage::NodePrepare(change.clone(), apply_when.clone());
                let mirrors = Stage::Mirrors(change);
                let mut stages = self.stages.borrow_mut();
                if node_first {
                    stages.extend([node, mirrors]);
                } else {
                    stages.extend([mirrors, node]);
                }
                drop(stages);
                self.wake_chain();
            }
            ControlAction::ProbeSend { frames } => self
                .to_probe
                .try_send(frames)
                .unwrap_or_else(|_| panic!("{full}")),
            ControlAction::InsertWork => self.topology.insert_work(),
            ControlAction::DispatchVerdict => self.topology.dispatch_verdict(),
            ControlAction::SubmitUnrequested => self.topology.submit_unrequested(),
            action => self
                .to_links
                .try_send(action)
                .unwrap_or_else(|_| panic!("{full}")),
        }
    }
}
