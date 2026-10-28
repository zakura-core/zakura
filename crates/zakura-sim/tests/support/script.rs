//! Scripted observations: a source that emits one batch per step, for
//! checking generated properties without a harness.

use std::{cell::RefCell, collections::VecDeque, rc::Rc};

use zakura_rt::task::yield_now;
use zakura_scheduler::SimInstant;
use zakura_sim::{
    property::{Assumptions, HasView, Property, Registry, RegistryChecker, Source, Sources},
    ActionSource, Boundary, Identity, Observation, ObservationLog, Plan, RunReport,
};

use super::{config, run};

/// The node, which the scripted observations stand for.
pub struct Node;

impl HasView for Node {
    type View = ();

    /// Nothing. Generated properties read observations.
    fn view(&self) {}
}

/// Emits one batch of observations per step.
struct Script(Rc<RefCell<VecDeque<Vec<Observation>>>>);

impl ActionSource for Script {
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if !self.0.borrow().is_empty() {
            out.push((Identity::peer("script", "next"), 0));
        }
    }

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, _: u64, log: &mut ObservationLog) {
        for obs in self.0.borrow_mut().pop_front().unwrap_or_default() {
            log.push(obs);
        }
    }
}

/// The sources a scripted run observes: frames, closes and storage, plus
/// the step loop's task observations.
pub const OBSERVED: &[Source] = &[
    Source::Frames,
    Source::Closes,
    Source::Storage,
    Source::Schedule,
];

/// Runs `script`, one batch per step, under `properties`, observing
/// `observed`. A `node` task stays ready until the script ends, so the run
/// reaches quiescence only after the last batch.
pub fn run_under(
    properties: Vec<Property<Node>>,
    observed: &[Source],
    script: Vec<Vec<Observation>>,
) -> RunReport {
    let mut checker = RegistryChecker::new(
        Registry::new(properties).expect("generated IDs are unique"),
        Rc::new(Node),
        Assumptions::none(),
        &Sources::observed(observed),
    );
    let batches = Rc::new(RefCell::new(VecDeque::from(script)));
    // Each step offers the task first, then the script: byte 1 picks the
    // script while it lasts.
    run(
        config(1, 1_000),
        Plan::from_fuzz(&[1; 64]),
        |sim| {
            let left = Rc::clone(&batches);
            sim.scheduler().spawn_root("node", async move {
                while !left.borrow().is_empty() {
                    yield_now().await;
                }
            });
            sim.add_source(Box::new(Script(batches)));
        },
        &mut checker,
    )
}
