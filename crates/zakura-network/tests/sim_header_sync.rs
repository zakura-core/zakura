//! The header-sync reactor on the deterministic scheduler. These only build
//! with `RUSTFLAGS="--cfg zakura_sim"`, in a separate `CARGO_TARGET_DIR`.
//!
//! Each reactor runs with inert storage. A small seeded `Plan` stands in for
//! `zakura-sim`: it picks which ready task runs and answers every random
//! draw.
#![cfg(zakura_sim)]

use std::{
    cell::RefCell,
    net::{Ipv4Addr, SocketAddr},
    rc::Rc,
    sync::Arc,
    time::Duration,
};

use zakura_chain::parameters::Network;
use zakura_network::{
    zakura::{
        framed_channel, header_sync_header_bytes_for_network, spawn_header_sync_reactor,
        v9::{
            self, Cursor, Headers, HeadersOutcome, HeadersOutcomeCode, Message, Operation,
            RegtestMessage, SubscribeHeaders,
        },
        wire_codec::{decode_frame, encode_frame},
        AuxSchema, Credit, FullStateFrontiers, HeaderEntry, HeaderSyncService, HeaderSyncStartup,
        MessageRule, Peer, Service, ServicePeerDirection, ServiceRegistry, ZakuraEndpoint,
        ZakuraHandshakeConfig, ZakuraHeaderSyncConfig, ZakuraLocalLimits, ZakuraPeerId,
        ZakuraProtocolHandler, ZakuraSupervisorHandle, ZakuraTrace, LOCAL_MAX_MESSAGE_BYTES,
        ZAKURA_CAP_HEADER_SYNC, ZAKURA_HEADER_SYNC_STREAM_VERSION, ZAKURA_STREAM_HEADER_SYNC,
    },
    Config,
};
use zakura_node_services::header_chain::InertHeaderChainPort;
use zakura_quic::{
    sim::{LinkModel, SeededInputs, SimNet},
    NodeAddr, NodeSecretKey, QuicBindConfig, QuicEndpoint,
};
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

/// The node's address.
fn node_addr() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::new(10, 0, 0, 1), 4000))
}

/// The peer's address.
fn peer_addr() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::new(10, 0, 0, 2), 4000))
}

/// The seed of every endpoint's keys and inputs.
const SEED: [u8; 32] = [5; 32];

/// One socket at `addr`.
fn bind_config(addr: SocketAddr) -> QuicBindConfig {
    QuicBindConfig {
        addrs: vec![addr],
        max_bidi_streams: 16,
        max_connections: 16,
        dial_reserve: 0,
        max_closing_connections: 16,
    }
}

/// Production network config on Regtest.
fn regtest_config() -> Config {
    Config {
        network: Network::new_regtest(Default::default()),
        ..Config::default()
    }
}

/// Builds the node: a header-sync reactor on inert storage behind the real
/// handler, which advertises header sync alone. Returns its QUIC endpoint,
/// handler and local limits.
fn bind_node(
    net: &SimNet,
    service: Arc<dyn Service>,
) -> (QuicEndpoint, ZakuraProtocolHandler, ZakuraLocalLimits) {
    let config = regtest_config();
    let limits = ZakuraLocalLimits::from_config(&config);
    let quic = QuicEndpoint::bind_with(
        SeededInputs::node_key(SEED, 0),
        &bind_config(node_addr()),
        &limits.quic,
        net.clone(),
        SeededInputs::new(SEED, 0).endpoint_inputs(),
    )
    .expect("the node binds");
    let supervisor = ZakuraSupervisorHandle::new(config.zakura.max_connections_per_ip);
    let registry = ServiceRegistry::new(vec![service]).expect("one service registers");
    let handler = ZakuraProtocolHandler::new_with_registry_and_trace(
        supervisor,
        config.network.clone(),
        ZakuraHandshakeConfig::for_network(&config.network),
        limits.clone(),
        Arc::new(registry),
        ZakuraTrace::noop(),
    )
    .with_local_node_id(quic.local_id());
    (quic, handler, limits)
}

/// `ZakuraEndpoint::new` and `ZakuraEndpoint::dial` build and run without
/// `zakura-testkit`. A dial to an address where nothing answers returns the
/// transport's handshake timeout unchanged.
#[test]
fn endpoint_new_builds_without_the_testkit() {
    let plan = Rc::new(RefCell::new(Plan(17)));
    let mut sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    {
        let plan = Rc::clone(&plan);
        sched.set_draws(move |draw| plan.borrow_mut().below(draw.bound));
    }
    let net = SimNet::new(LinkModel::default());
    let shutdown = CancellationToken::new();
    let (handle, actions, reactor) =
        spawn_header_sync_reactor(startup(shutdown.clone())).expect("the reactor starts");
    sched.spawn_root("reactor", async move {
        reactor.await.expect("the reactor ends on shutdown");
    });
    let service: Arc<dyn Service> = Arc::new(HeaderSyncService::new(handle));
    let dialed = Rc::new(RefCell::new(None));
    {
        let (net, dialed) = (net.clone(), Rc::clone(&dialed));
        sched.spawn_root("node", async move {
            let _actions = actions;
            let (quic, handler, limits) = bind_node(&net, service);
            let endpoint = ZakuraEndpoint::new(quic, handler);
            let unreachable =
                NodeAddr::with_addrs(SeededInputs::node_key(SEED, 1).public(), [peer_addr()]);
            let result = endpoint.dial(unreachable, &limits).await;
            *dialed.borrow_mut() = Some(format!("{result:?}"));
            endpoint.shutdown().await;
        });
    }
    let mut steps = Vec::new();
    run_to_end(&mut sched, &plan, &mut steps);
    shutdown.cancel();
    run_to_end(&mut sched, &plan, &mut steps);
    let _ = sched.shutdown();
    let dialed = dialed.borrow_mut().take().expect("the dial returned");
    // The transport's handshake timeout comes back as it is, not wrapped as
    // the dial's own deadline.
    assert_eq!(dialed, "Err(QuicConnect(HandshakeTimeout))");
}

/// `ZakuraEndpoint::shutdown` stops a maintained dial started on an endpoint
/// from `ZakuraEndpoint::new`, even while it is still retrying.
#[test]
fn shutdown_stops_a_maintained_dial() {
    let plan = Rc::new(RefCell::new(Plan(17)));
    let mut sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    {
        let plan = Rc::clone(&plan);
        sched.set_draws(move |draw| plan.borrow_mut().below(draw.bound));
    }
    let net = SimNet::new(LinkModel::default());
    let shutdown = CancellationToken::new();
    let (handle, actions, reactor) =
        spawn_header_sync_reactor(startup(shutdown.clone())).expect("the reactor starts");
    sched.spawn_root("reactor", async move {
        reactor.await.expect("the reactor ends on shutdown");
    });
    let service: Arc<dyn Service> = Arc::new(HeaderSyncService::new(handle));
    let stopped = Rc::new(RefCell::new(false));
    {
        let (net, stopped) = (net.clone(), Rc::clone(&stopped));
        sched.spawn_root("node", async move {
            let _actions = actions;
            let (quic, handler, _limits) = bind_node(&net, service);
            let endpoint = ZakuraEndpoint::new(quic, handler);
            let unreachable =
                NodeAddr::with_addrs(SeededInputs::node_key(SEED, 1).public(), [peer_addr()]);
            let dial = endpoint.spawn_native_dial(unreachable);
            // Long enough for at least one failed attempt and a backoff.
            zakura_rt::time::sleep(Duration::from_secs(60)).await;
            assert!(!dial.is_finished(), "the dial is still retrying");
            endpoint.shutdown().await;
            dial.await.expect("the dial task ends without panicking");
            *stopped.borrow_mut() = true;
        });
    }
    let mut steps = Vec::new();
    run_to_end(&mut sched, &plan, &mut steps);
    shutdown.cancel();
    run_to_end(&mut sched, &plan, &mut steps);
    let _ = sched.shutdown();
    assert!(*stopped.borrow(), "the maintained dial ended at shutdown");
}

/// Every version 9 message builds from public fields, encodes, decodes to
/// itself, and reads back field by field.
#[test]
fn codec_builds_every_row_from_outside_the_crate() {
    let genesis = zakura_chain::block::genesis::regtest_genesis_block();
    let parent = (zakura_chain::block::Height(0), genesis.hash());
    let mut header = *genesis.header;
    header.previous_block_hash = parent.1;
    let target = header.hash();
    let status = v9::Status {
        work_anchor_height: parent.0,
        work_anchor_hash: parent.1,
        selected_tip_height: zakura_chain::block::Height(1),
        selected_tip_hash: target,
        suffix_cumulative_work: Default::default(),
        oldest_retained_height: parent.0,
        max_headers_per_response: 160,
        max_subscriptions: 1,
        max_message_bytes: 1 << 20,
        tree_aux_schema_mask: 0,
    };
    let cursor = Cursor {
        height: parent.0,
        hash: parent.1,
        headers: 0,
        bytes: 0,
    };
    let open = SubscribeHeaders {
        operation: Operation::Open,
        subscription_id: 1,
        update_sequence: 0,
        initial_target_tip_hash: target,
        acknowledged: cursor,
        locator_hashes: vec![parent.1],
        added_credit: Credit {
            objects: 10,
            bytes: 10_000,
        },
        tree_aux_schema: AuxSchema::None,
    };
    let page = Headers {
        subscription_id: 1,
        initial_target_tip_hash: target,
        common_ancestor_height: parent.0,
        common_ancestor_hash: parent.1,
        reaches_initial_target: true,
        tree_aux_schema: AuxSchema::None,
        entries: vec![HeaderEntry {
            header: Arc::new(header),
            body_size: 1_000,
            tree_aux: None,
        }],
    };
    let outcome = HeadersOutcome {
        subscription_id: 1,
        initial_target_tip_hash: target,
        outcome: HeadersOutcomeCode::SubscriptionClosed,
    };
    let messages = [
        RegtestMessage::Status(status.clone()),
        RegtestMessage::SubscribeHeaders(open.clone()),
        RegtestMessage::Headers(page.clone()),
        RegtestMessage::HeadersOutcome(outcome),
    ];
    for (message, row) in messages.iter().zip(RegtestMessage::ROWS) {
        let frame = encode_frame(message).expect("the message encodes");
        assert_eq!(frame.message_type, row.message_type, "{}", row.name);
        let decoded = decode_frame::<RegtestMessage>(&frame).expect("the frame decodes");
        assert_eq!(&decoded, message, "{}", row.name);
        match decoded {
            Message::Status(decoded) => {
                assert_eq!(decoded.selected_tip_hash, status.selected_tip_hash);
                assert_eq!(decoded.max_headers_per_response, 160);
                assert_eq!(decoded.max_message_bytes, 1 << 20);
            }
            Message::SubscribeHeaders(decoded) => {
                assert_eq!(decoded.operation, Operation::Open);
                assert_eq!(decoded.acknowledged, cursor);
                assert_eq!(decoded.locator_hashes, open.locator_hashes);
                assert_eq!(decoded.added_credit, open.added_credit);
            }
            Message::Headers(decoded) => {
                assert_eq!(decoded.common_ancestor_hash, parent.1);
                assert!(decoded.reaches_initial_target);
                assert_eq!(decoded.entries[0].header.hash(), target);
                assert_eq!(decoded.entries[0].body_size, 1_000);
            }
            Message::HeadersOutcome(decoded) => {
                assert_eq!(decoded.outcome, HeadersOutcomeCode::SubscriptionClosed);
                assert_eq!(decoded.initial_target_tip_hash, target);
            }
        }
    }
    let network = Network::new_regtest(Default::default());
    let names = |rows: &[MessageRule]| rows.iter().map(|row| row.name).collect::<Vec<_>>();
    assert_eq!(
        names(v9::rows(header_sync_header_bytes_for_network(&network))),
        names(&RegtestMessage::ROWS)
    );
    assert_eq!(
        v9::layout(&network)[0].messages.map(names),
        Some(names(&RegtestMessage::ROWS))
    );
}
