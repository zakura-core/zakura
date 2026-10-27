//! The header-sync reactor on the deterministic scheduler. These only build
//! with `RUSTFLAGS="--cfg zakura_sim"`, in a separate `CARGO_TARGET_DIR`.
//!
//! Each reactor runs with inert storage. A small seeded `Plan` stands in for
//! `zakura-sim`: it picks which ready task runs and answers every random
//! draw.
#![cfg(zakura_sim)]

use std::{cell::RefCell, rc::Rc, sync::Arc};

use zakura_chain::parameters::Network;
use zakura_network::zakura::{
    framed_channel, spawn_header_sync_reactor, FullStateFrontiers, HeaderSyncService,
    HeaderSyncStartup, Peer, Service, ServicePeerDirection, ZakuraHeaderSyncConfig, ZakuraPeerId,
    LOCAL_MAX_MESSAGE_BYTES, ZAKURA_CAP_HEADER_SYNC, ZAKURA_HEADER_SYNC_STREAM_VERSION,
    ZAKURA_STREAM_HEADER_SYNC,
};
use zakura_node_services::header_chain::InertHeaderChainPort;
use zakura_quic::NodeSecretKey;
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

/// Runs ready tasks, picked by `plan`, advancing time whenever they only
/// wait on it, until the run stops for any other reason.
fn run_to_end(sched: &mut Scheduler, plan: &RefCell<Plan>, steps: &mut Vec<String>) -> Status {
    loop {
        match run_until_idle(sched, plan, steps) {
            Status::WaitingOnTime => {
                let next = sched.next_deadline().expect("WaitingOnTime has a timer");
                sched.advance_to(next);
                steps.push("advance-time".to_owned());
            }
            other => return other,
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
    let end = run_to_end(&mut sched, &plan, &mut steps);
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

/// The peer identity of the node whose key is `seed` repeated.
fn peer_id(seed: u8) -> ZakuraPeerId {
    let node = NodeSecretKey::from_bytes(&[seed; 32]).public();
    ZakuraPeerId::new(node.as_bytes().to_vec()).expect("a node id is a valid peer id")
}

/// Two reactors, `a` and `b`, each behind its own service. `a` dials
/// `b`, so each service gets one versioned session over a pair of
/// in-memory streams, one each way.
#[test]
fn two_reactors_meet_over_memory_streams() {
    let plan = Rc::new(RefCell::new(Plan(3)));
    let mut sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    {
        let plan = Rc::clone(&plan);
        sched.set_draws(move |draw| plan.borrow_mut().below(draw.bound));
    }

    let shutdown = CancellationToken::new();
    let exits = Rc::new(RefCell::new(Vec::new()));
    let mut nodes = Vec::new();
    for name in ["a", "b"] {
        let (handle, actions, reactor) =
            spawn_header_sync_reactor(startup(shutdown.clone())).expect("the reactor starts");
        let exits = Rc::clone(&exits);
        sched.spawn_root(name, async move {
            let exit = match reactor.await {
                Ok(()) => "completed".to_owned(),
                Err(error) => format!("{error}"),
            };
            exits.borrow_mut().push(exit);
        });
        let service = HeaderSyncService::new(handle.clone());
        nodes.push((handle, actions, service));
    }

    // `a` writes what `b` reads, and the other way round.
    let (a_send, b_recv) = framed_channel(8);
    let (b_send, a_recv) = framed_channel(8);
    let connections = [CancellationToken::new(), CancellationToken::new()];
    let sides = [
        (peer_id(2), ServicePeerDirection::Outbound, (a_recv, a_send)),
        (peer_id(1), ServicePeerDirection::Inbound, (b_recv, b_send)),
    ];
    for (((remote, direction, streams), (_, _, service)), connection) in
        sides.into_iter().zip(&nodes).zip(&connections)
    {
        service.add_peer(Peer::new_versioned(
            1,
            remote,
            ZAKURA_CAP_HEADER_SYNC,
            direction,
            ZAKURA_STREAM_HEADER_SYNC,
            ZAKURA_HEADER_SYNC_STREAM_VERSION,
            1,
            streams,
            connection.clone(),
        ));
    }

    let mut steps = Vec::new();
    let settled = run_until_idle(&mut sched, &plan, &mut steps);
    assert_eq!(settled, Status::WaitingOnTime, "{steps:?}");
    let connected: Vec<_> = nodes
        .iter()
        .map(|(handle, _, _)| {
            let snapshot = handle.peer_snapshot();
            (snapshot.outbound_peers, snapshot.inbound_peers)
        })
        .collect();
    assert_eq!(connected, [(1, 0), (0, 1)], "each reactor admits its peer");
    assert!(
        connections
            .iter()
            .all(|connection| !connection.is_cancelled()),
        "neither service refused its session"
    );

    // A node shutdown stops the reactors and closes the transport's
    // connections, which ends each session's tasks.
    shutdown.cancel();
    for connection in &connections {
        connection.cancel();
    }
    let end = run_to_end(&mut sched, &plan, &mut steps);
    assert_eq!(end, Status::Quiescent, "every task ends after shutdown");
    drop(nodes);
    assert_eq!(sched.shutdown(), Default::default(), "no task was left");
    assert_eq!(*exits.borrow(), ["completed", "completed"]);
    assert!(
        !steps.iter().any(|step| step.starts_with("panicked")),
        "{steps:?}"
    );
}

/// Reactor `a`'s session with peer `b` is replaced while its pipe still runs. The
/// reactor matches a disconnect by peer and session ID alone, so the
/// replacement's fresh session ID is what keeps the old session's late
/// teardown from removing it.
#[test]
fn a_replacement_session_outlives_the_old_teardown() {
    let plan = Rc::new(RefCell::new(Plan(5)));
    let mut sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    {
        let plan = Rc::clone(&plan);
        sched.set_draws(move |draw| plan.borrow_mut().below(draw.bound));
    }

    let shutdown = CancellationToken::new();
    let (handle, actions, reactor) =
        spawn_header_sync_reactor(startup(shutdown.clone())).expect("the reactor starts");
    sched.spawn_root("a", async move {
        reactor.await.expect("the reactor ends on shutdown");
    });
    let service = HeaderSyncService::new(handle.clone());

    // Opens a session on a new connection, and returns the connection and a
    // flag set once the service drops the session's send half.
    let connect = |sched: &mut Scheduler, conn_id, session_id| {
        let (send, mut far_recv) = framed_channel(8);
        let (far_send, recv) = framed_channel(8);
        let connection = CancellationToken::new();
        service.add_peer(Peer::new_versioned(
            conn_id,
            peer_id(2),
            ZAKURA_CAP_HEADER_SYNC,
            ServicePeerDirection::Outbound,
            ZAKURA_STREAM_HEADER_SYNC,
            ZAKURA_HEADER_SYNC_STREAM_VERSION,
            session_id,
            (recv, send),
            connection.clone(),
        ));
        let ended = Rc::new(RefCell::new(false));
        let flag = Rc::clone(&ended);
        sched.spawn_root("b", async move {
            // Keeps `a`'s reader open until the run ends.
            let _far_send = far_send;
            while far_recv.recv().await.is_some() {}
            *flag.borrow_mut() = true;
        });
        (connection, ended)
    };

    let mut steps = Vec::new();
    let old = connect(&mut sched, 1, 1);
    assert_eq!(
        run_until_idle(&mut sched, &plan, &mut steps),
        Status::WaitingOnTime
    );
    assert_eq!(handle.peer_snapshot().outbound_peers, 1, "{steps:?}");

    let replacement = connect(&mut sched, 2, 2);
    assert_eq!(
        run_until_idle(&mut sched, &plan, &mut steps),
        Status::WaitingOnTime
    );
    assert!(*old.1.borrow(), "the old session tore down: {steps:?}");
    assert_eq!(
        handle.peer_snapshot().outbound_peers,
        1,
        "the replacement is still admitted"
    );
    assert!(!*replacement.1.borrow(), "the replacement still runs");

    shutdown.cancel();
    old.0.cancel();
    replacement.0.cancel();
    let end = run_to_end(&mut sched, &plan, &mut steps);
    assert_eq!(end, Status::Quiescent, "every task ends after shutdown");
    drop((service, handle, actions));
    assert_eq!(sched.shutdown(), Default::default(), "no task was left");
    assert!(
        !steps.iter().any(|step| step.starts_with("panicked")),
        "{steps:?}"
    );
}
