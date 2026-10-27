//! Scripted harness actions on a block-sync run's links and chain.
//!
//! [`Control`] offers a scenario's [`ControlStep`]s in order, each once, so
//! the `Plan` decides when each happens, once its trigger holds. A harness
//! task carries out each link, probe and mirror action on its next step,
//! since those take locks that tasks hold across scheduling points, where
//! only a task can wait. So a pick only queues the action, and triggers
//! count the effect. A chain change reaches the node under test last, as a
//! step of its own: the step loop alone reads the node's engine.

use std::{cell::RefCell, collections::VecDeque, rc::Rc};

use serde::{Deserialize, Serialize};
use zakura_rt::sync::mpsc;
use zakura_scheduler::SimInstant;

use super::{
    policy::{Predicates, Trigger},
    probe::ProbeFrame,
    ChainChange, Topology, PROBE,
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
    /// The height the fork extends, at most the chain's tip.
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
    /// headers on each engine, and their bodies in each mirror's full state,
    /// the mirrors first. The node under test must download the bodies.
    Grow {
        /// The new blocks.
        len: u32,
    },
    /// Switch every actor to `fork`: its headers on each engine, which
    /// reorganizes the node's selected chain through its real engine, and
    /// its bodies in each mirror's full state, the mirrors first. The
    /// node's verified chain resets once it commits the fork's bodies over
    /// its own.
    Reset {
        /// The fork.
        fork: Fork,
    },
    /// Send `frames` from the probe, in order. The scenario must link one.
    ProbeSend {
        /// The frames.
        frames: Vec<ProbeFrame>,
    },
}

impl ControlAction {
    /// Who the action's identity names: a peer, or `chain` for a change to
    /// every actor's chain.
    fn actor(&self) -> &str {
        match self {
            ControlAction::Close { peer } | ControlAction::Relink { peer } => peer,
            ControlAction::Grow { .. } | ControlAction::Reset { .. } => "chain",
            ControlAction::ProbeSend { .. } => PROBE,
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

/// What the control task does.
enum Work {
    /// A link or probe action.
    Act(ControlAction),
    /// A chain change, on the mirrors.
    Change(ChainChange),
}

/// The token of the step that makes a chain change on the node.
const CHANGE_NODE: u64 = u64::MAX;

/// Offers a scenario's scripted actions in order, each once, as
/// `peer:<actor>:<action>`, and each chain change the mirrors have made as
/// `peer:node:<action>`. See the module doc.
pub struct Control {
    script: Vec<ControlStep>,
    topology: Rc<Topology>,
    predicates: Rc<RefCell<Predicates>>,
    /// The next step to offer.
    next: usize,
    /// The chain changes the mirrors have made and the node hasn't, in
    /// order.
    changed: Rc<RefCell<VecDeque<ChainChange>>>,
    to_task: mpsc::Sender<Work>,
}

impl Control {
    /// A source for `script` on `topology`, gated by `predicates`, with its
    /// task spawned on `sim`.
    pub fn new(
        sim: &mut Sim,
        topology: Rc<Topology>,
        script: Vec<ControlStep>,
        predicates: Rc<RefCell<Predicates>>,
    ) -> Self {
        let (to_task, mut work) = mpsc::channel::<Work>(script.len().max(1));
        let changed = Rc::new(RefCell::new(VecDeque::new()));
        let task_topology = topology.clone();
        let task_changed = changed.clone();
        sim.scheduler().spawn_root("control", async move {
            let topology = task_topology;
            while let Some(work) = work.recv().await {
                match work {
                    Work::Act(ControlAction::Close { peer }) => topology
                        .link_to(&peer)
                        .unwrap_or_else(|| panic!("the scenario links `{peer}`"))
                        .close(),
                    Work::Act(ControlAction::Relink { peer }) => topology.relink(&peer),
                    Work::Act(ControlAction::ProbeSend { frames }) => {
                        topology.probe_send(&frames).await
                    }
                    Work::Act(action) => unreachable!("`Control` plans {action:?} itself"),
                    Work::Change(change) => {
                        topology.change_mirrors(&change).await;
                        task_changed.borrow_mut().push_back(change);
                    }
                }
            }
        });
        Control {
            script,
            topology,
            predicates,
            next: 0,
            changed,
            to_task,
        }
    }
}

impl ActionSource for Control {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if let Some(change) = self.changed.borrow().front() {
            let action = match change.effect {
                LinkEffect::Reset => "reset",
                _ => "grow",
            };
            out.push((Identity::peer("node", action), CHANGE_NODE));
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
            let change = self
                .changed
                .borrow_mut()
                .pop_front()
                .expect("`enabled` offered a change the mirrors made");
            self.topology.change_node(&change);
            return;
        }
        let action = self.script[self.next].action.clone();
        self.next += 1;
        let work = match action {
            ControlAction::Grow { .. } | ControlAction::Reset { .. } => {
                match self.topology.plan_change(&action) {
                    Some(change) => Work::Change(change),
                    None => return,
                }
            }
            action => Work::Act(action),
        };
        self.to_task.try_send(work).unwrap_or_else(|_| {
            panic!("the channel holds the whole script, and the task outlives the run")
        });
    }
}
