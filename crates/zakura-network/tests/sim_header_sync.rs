//! The header-sync reactor on the deterministic scheduler. These only build
//! with `RUSTFLAGS="--cfg zakura_sim"`, in a separate `CARGO_TARGET_DIR`.
//!
//! The reactor runs with inert storage and no peers. A small seeded `Plan`
//! stands in for `zakura-sim`: it picks which ready task runs and answers
//! every random draw.
#![cfg(zakura_sim)]

use std::{cell::RefCell, rc::Rc, sync::Arc};

use zakura_chain::parameters::Network;
use zakura_network::zakura::{
    spawn_header_sync_reactor, FullStateFrontiers, HeaderSyncStartup, ZakuraHeaderSyncConfig,
    LOCAL_MAX_MESSAGE_BYTES,
};
use zakura_node_services::header_chain::InertHeaderChainPort;
use zakura_rt::sync::CancellationToken;
use zakura_scheduler::{RunOutcome, Scheduler, SchedulerConfig, SimInstant, Status};

/// A run that takes more steps than this is stuck.
const MAX_STEPS: usize = 100_000;

/// A seeded stream of choices: which ready task runs, and every draw's
/// answer.
struct Plan(u64);

impl Plan {
    /// The next 64 bits (SplitMix64).
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A choice below `bound`, or any `u64` when `bound` is 0.
    fn below(&mut self, bound: u64) -> u64 {
        match bound {
            0 => self.next(),
            bound => self.next() % bound,
        }
    }
}

/// Everything a run lets a test see, in order.
#[derive(Debug, PartialEq, Eq)]
struct Observations {
    /// Each step as `task:kind#n`, `advance-time`, `shutdown`, or a panic.
    steps: Vec<String>,
    /// Each draw as `kind/bound`.
    draws: Vec<String>,
    /// Where the run stood before shutdown was requested.
    settled: Status,
    /// How the reactor's task ended.
    exit: String,
}

/// Header-sync startup on Regtest at genesis, with inert storage.
fn startup(shutdown: CancellationToken) -> HeaderSyncStartup {
    let network = Network::new_regtest(Default::default());
    let anchor = (zakura_chain::block::Height(0), network.genesis_hash());
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
    startup.shutdown = shutdown;
    startup
}

/// Runs ready tasks, picked by `plan`, until none is ready. Records each
/// step in `steps`.
fn run_until_idle(sched: &mut Scheduler, plan: &RefCell<Plan>, steps: &mut Vec<String>) -> Status {
    loop {
        assert!(steps.len() < MAX_STEPS, "the run took {MAX_STEPS} steps");
        let status = sched.status();
        if status != Status::Running {
            return status;
        }
        let ready = sched.ready();
        let index = plan.borrow_mut().below(ready.len() as u64); // a Vec's length fits in u64
        let pick = ready[index as usize]; // below `ready.len()`, a usize
        steps.push(format!("{}:{}", sched.task_name(pick.task), pick.at));
        if let RunOutcome::Panicked(message) = sched.run(pick.task) {
            steps.push(format!("panicked: {message}"));
        }
    }
}

/// Starts the reactor, runs it until it only waits on time, then shuts it
/// down and runs it to the end.
fn run(seed: u64) -> Observations {
    let plan = Rc::new(RefCell::new(Plan(seed)));
    let draws = Rc::new(RefCell::new(Vec::new()));
    let mut sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    {
        let (plan, draws) = (Rc::clone(&plan), Rc::clone(&draws));
        sched.set_draws(move |draw| {
            draws
                .borrow_mut()
                .push(format!("{:?}/{}", draw.kind, draw.bound));
            plan.borrow_mut().below(draw.bound)
        });
    }

    let shutdown = CancellationToken::new();
    let (handle, actions, reactor) =
        spawn_header_sync_reactor(startup(shutdown.clone())).expect("the reactor starts");
    let exit = Rc::new(RefCell::new(None));
    {
        let exit = Rc::clone(&exit);
        sched.spawn_root("join", async move {
            *exit.borrow_mut() = Some(match reactor.await {
                Ok(()) => "completed".to_owned(),
                Err(error) => format!("{error}"),
            });
        });
    }

    let mut steps = Vec::new();
    let settled = run_until_idle(&mut sched, &plan, &mut steps);

    steps.push("shutdown".to_owned());
    shutdown.cancel();
    let end = loop {
        match run_until_idle(&mut sched, &plan, &mut steps) {
            Status::WaitingOnTime => {
                let next = sched.next_deadline().expect("WaitingOnTime has a timer");
                sched.advance_to(next);
                steps.push("advance-time".to_owned());
            }
            other => break other,
        }
    };
    assert_eq!(end, Status::Quiescent, "every task ends after shutdown");
    // The reactor's inputs stay alive until it has ended on its own.
    drop((handle, actions));
    let report = sched.shutdown();
    assert_eq!(report, Default::default(), "nothing was left to clean up");

    let exit = exit
        .borrow_mut()
        .take()
        .expect("the join task saw the reactor end");
    Observations {
        steps,
        draws: draws.take(),
        settled,
        exit,
    }
}

#[test]
fn reactor_starts_waits_on_time_and_shuts_down() {
    let observed = run(1);
    assert_eq!(observed.settled, Status::WaitingOnTime);
    assert_eq!(observed.exit, "completed");
    assert!(
        observed
            .steps
            .iter()
            .any(|step| step.starts_with("header_sync_reactor.1:")),
        "the reactor ran under its spawn name: {:?}",
        observed.steps
    );
    assert!(
        !observed
            .steps
            .iter()
            .any(|step| step.starts_with("panicked")),
        "{:?}",
        observed.steps
    );
}

#[test]
fn the_same_plan_replays_the_same_run() {
    let first = run(7);
    assert_eq!(first, run(7), "a second run with the same plan");
    let other = run(8);
    assert_eq!(first, run(7), "a run after an unrelated one");
    // Plan 8 orders the first steps differently, so the comparisons above
    // would catch a run that ignored its plan.
    assert_ne!(first, other);
}
