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
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::future::BoxFuture;
use zakura_chain::parameters::Network;
use zakura_network::{
    zakura::{
        framed_channel, header_sync_header_bytes_for_network, spawn_header_sync_reactor,
        v9::{
            self, Cursor, Headers, HeadersOutcome, HeadersOutcomeCode, Message, Operation,
            RegtestMessage, SubscribeHeaders,
        },
        wire_codec::{decode_frame, encode_frame},
        zakura_protocol_for_alpn, AuxSchema, Credit, FullStateFrontiers, HeaderEntry,
        HeaderSyncService, HeaderSyncStartup, MessageRule, Peer, Service, ServicePeerDirection,
        ServiceRegistry, StreamPrelude, TransportEvent, TransportObserver, ZakuraControlAck,
        ZakuraControlHello, ZakuraControlRole, ZakuraEndpoint, ZakuraHandshakeConfig,
        ZakuraHandshakePath, ZakuraHeaderSyncConfig, ZakuraLocalLimits, ZakuraPeerId,
        ZakuraProtocolHandler, ZakuraSupervisorHandle, ZakuraTrace, CONTROL_ACK_MAGIC,
        CONTROL_HELLO_MAGIC, CONTROL_VERSION, LOCAL_MAX_MESSAGE_BYTES, P2P_V2_ALPN,
        STREAM_PRELUDE_MAGIC, ZAKURA_CAP_HEADER_SYNC, ZAKURA_HEADER_SYNC_STREAM_VERSION,
        ZAKURA_STREAM_HEADER_SYNC,
    },
    Config,
};
use zakura_node_services::header_chain::InertHeaderChainPort;
use zakura_quic::{
    sim::{LinkModel, SeededInputs, SimNet},
    Acceptor, Admit, Conn, IncomingInfo, NodeAddr, NodeId, NodeSecretKey, QuicBindConfig,
    QuicConfig, QuicEndpoint,
};
use zakura_rt::sync::CancellationToken;
use zakura_scheduler::{Op, RunOutcome, Scheduler, SchedulerConfig, SimInstant, Status};

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

// A header-sync node behind the real handler on `SimNet`, a raw peer that
// runs the control handshake from the public wire types, and a transport
// observer. The node runs production limits and timeouts.

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

/// Records every transport event in order.
#[derive(Debug, Default)]
struct Recorder(Mutex<Vec<TransportEvent>>);

impl TransportObserver for Recorder {
    fn observe(&self, event: TransportEvent) {
        self.0
            .lock()
            .expect("no test panics while holding it")
            .push(event);
    }
}

impl Recorder {
    /// The events recorded so far.
    fn events(&self) -> Vec<TransportEvent> {
        self.0
            .lock()
            .expect("no test panics while holding it")
            .clone()
    }
}

/// Converts a runtime instant to scheduler time, given one moment read
/// from both clocks.
fn sim_of(instant: std::time::Instant, now_std: std::time::Instant, now: SimInstant) -> SimInstant {
    SimInstant::from_start(now.since_start() + instant.saturating_duration_since(now_std))
}

/// The node and its scheduler, driven step by step.
struct Wire {
    sched: Scheduler,
    plan: Rc<RefCell<Plan>>,
    net: SimNet,
    recorder: Arc<Recorder>,
    /// Ends the peer's script and both endpoints.
    stop: CancellationToken,
    /// Starts the peer's second write.
    go: CancellationToken,
    shutdown: CancellationToken,
    /// Tasks whose own name, the last part of their path, starts with this
    /// aren't run.
    held: Option<&'static str>,
    /// Whether to start holding the node's serving task once it's ready at a
    /// `watch` point in the handler, which it first reaches inside
    /// `register`, as the supervisor publishes its peer set.
    hold_in_register: bool,
    steps: Vec<String>,
}

/// Builds the node: a header-sync reactor on inert storage behind the real
/// handler, which advertises header sync alone and reports to `recorder`, if
/// there is one. Returns its QUIC endpoint, handler and local limits.
fn bind_node(
    net: &SimNet,
    service: Arc<dyn Service>,
    recorder: Option<Arc<Recorder>>,
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
    let handler = match recorder {
        Some(recorder) => handler.with_transport_observer(recorder),
        None => handler,
    };
    (quic, handler, limits)
}

/// What the peer writes.
#[derive(Clone, Debug, Default)]
struct Script {
    /// A stream kind and version to open before the header stream.
    other_stream: Option<(u16, u16)>,
    /// The bytes after the header stream's prelude.
    first: Vec<u8>,
    /// The bytes it writes once the test starts it.
    second: Vec<u8>,
    /// Whether the node's supervisor disconnects the peer once the test
    /// starts it.
    disconnect: bool,
    /// Whether the node's endpoint shuts down once the test starts it.
    shutdown: bool,
}

/// Dials the node from the peer, runs the control handshake with header
/// sync as the only capability, and opens a header stream with its prelude.
/// Writes `script`'s first bytes on it, then its second once `go` is
/// cancelled, and keeps the connection until `stop` is cancelled.
async fn peer_script(
    net: SimNet,
    node: NodeId,
    script: Script,
    go: CancellationToken,
    stop: CancellationToken,
) {
    let config = regtest_config();
    let limits = ZakuraLocalLimits::from_config(&config);
    let quic = QuicEndpoint::bind_with(
        SeededInputs::node_key(SEED, 1),
        &bind_config(peer_addr()),
        &QuicConfig::default(),
        net,
        SeededInputs::new(SEED, 1).endpoint_inputs(),
    )
    .expect("the peer binds");
    let conn = quic
        .connect(NodeAddr::with_addrs(node, [node_addr()]), &[P2P_V2_ALPN])
        .await
        .expect("the peer connects");
    let handshake = ZakuraHandshakeConfig::for_network(&config.network);
    let hello = ZakuraControlHello {
        magic: CONTROL_HELLO_MAGIC,
        control_version: CONTROL_VERSION,
        selected_zakura_protocol: zakura_protocol_for_alpn(conn.alpn())
            .expect("the connection's ALPN is a Zakura protocol"),
        handshake_path: ZakuraHandshakePath::Native,
        role: ZakuraControlRole::Initiator,
        network_id: handshake.network_id,
        chain_id: handshake.chain_id,
        iroh_node_id: quic.local_id().as_bytes().to_vec(),
        peer_nonce: [7; 32],
        initiator_upgrade_nonce: [0; 32],
        responder_upgrade_nonce: [0; 32],
        legacy_upgrade_transcript: [0; 32],
        capabilities: ZAKURA_CAP_HEADER_SYNC,
        required_channels: 0,
        initial_limits: limits.initial_limits(),
    };
    let (mut send, mut recv) = conn.open_bi().await.expect("the control stream opens");
    let hello = hello.encode().expect("the hello encodes");
    let len = u32::try_from(hello.len()).expect("a hello fits u32");
    send.write_all(&len.to_le_bytes())
        .await
        .expect("the hello's length is written");
    send.write_all(&hello).await.expect("the hello is written");
    let _ = send.finish();
    let mut len = [0; 4];
    recv.read_exact(&mut len)
        .await
        .expect("the ack's length arrives");
    let mut ack = vec![0; usize::try_from(u32::from_le_bytes(len)).expect("u32 fits usize")];
    recv.read_exact(&mut ack).await.expect("the ack arrives");
    let ack = ZakuraControlAck::decode(&ack).expect("the ack decodes");
    assert_eq!(ack.remote_peer_nonce, [7; 32], "the ack echoes the nonce");
    assert_eq!(ack.accepted_capabilities, ZAKURA_CAP_HEADER_SYNC);

    let mut other = None;
    if let Some((stream_kind, stream_version)) = script.other_stream {
        let (mut send, recv) = conn.open_bi().await.expect("the other stream opens");
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind,
            stream_version,
            request_id: None,
            max_frame_bytes: RegtestMessage::LAYOUT[0].frame_cap,
        };
        send.write_all(&prelude.encode().expect("the prelude encodes"))
            .await
            .expect("the other prelude is written");
        other = Some((send, recv));
    }
    let (mut send, _recv) = conn.open_bi().await.expect("the header stream opens");
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind: ZAKURA_STREAM_HEADER_SYNC,
        stream_version: ZAKURA_HEADER_SYNC_STREAM_VERSION,
        request_id: None,
        max_frame_bytes: RegtestMessage::LAYOUT[0].frame_cap,
    };
    let prelude = prelude.encode().expect("the prelude encodes");
    send.write_all(&[prelude, script.first].concat())
        .await
        .expect("the first bytes are written");
    go.cancelled().await;
    if !script.second.is_empty() {
        // The node may already have closed.
        let _ = send.write_all(&script.second).await;
    }
    stop.cancelled().await;
    drop((send, other));
    conn.close(0u32.into(), b"done");
    quic.shutdown().await;
}

impl Wire {
    /// Starts the node and a peer that runs `script`, which writes its second
    /// bytes after [`Self::go`] is cancelled.
    fn start(seed: u64, script: Script) -> Self {
        Self::start_observed(seed, script, true)
    }

    /// [`Self::start`], with the node's transport observer installed only if
    /// `observed`.
    fn start_observed(seed: u64, script: Script, observed: bool) -> Self {
        let plan = Rc::new(RefCell::new(Plan(seed)));
        let mut sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
        {
            let plan = Rc::clone(&plan);
            sched.set_draws(move |draw| plan.borrow_mut().below(draw.bound));
        }
        let net = SimNet::new(LinkModel::default());
        let recorder = Arc::new(Recorder::default());
        let shutdown = CancellationToken::new();
        let stop = CancellationToken::new();
        let go = CancellationToken::new();
        let (handle, actions, reactor) =
            spawn_header_sync_reactor(startup(shutdown.clone())).expect("the reactor starts");
        sched.spawn_root("reactor", async move {
            reactor.await.expect("the reactor ends on shutdown");
        });
        let service: Arc<dyn Service> = Arc::new(HeaderSyncService::new(handle));
        {
            let (net, recorder, stop, go) =
                (net.clone(), recorder.clone(), stop.clone(), go.clone());
            sched.spawn_root("node", async move {
                let _actions = actions;
                let recorder = observed.then_some(recorder);
                let (quic, handler, _limits) = bind_node(&net, service, recorder);
                quic.serve(handler.clone()).expect("the node serves");
                let node = quic.local_id();
                let endpoint = ZakuraEndpoint::new(quic, handler);
                let supervisor = endpoint.supervisor();
                let (disconnect, shutdown) = (script.disconnect, script.shutdown);
                let disconnecting = {
                    let (go, endpoint) = (go.clone(), endpoint.clone());
                    async move {
                        if disconnect || shutdown {
                            go.cancelled().await;
                        }
                        if disconnect {
                            assert!(supervisor.disconnect_peer(&wire_peer()).await);
                        }
                        if shutdown {
                            endpoint.shutdown().await;
                        }
                    }
                };
                futures::future::join(
                    peer_script(net, node, script, go, stop.clone()),
                    disconnecting,
                )
                .await;
                endpoint.shutdown().await;
            });
        }
        Self {
            sched,
            plan,
            net,
            recorder,
            stop,
            go,
            shutdown,
            held: None,
            hold_in_register: false,
            steps: Vec::new(),
        }
    }

    /// Runs ready tasks, delivers datagrams, and advances time, in `plan`'s
    /// order, until nothing is left at or before `until` since the start.
    fn run_until(&mut self, until: Duration) {
        loop {
            assert!(
                self.steps.len() < MAX_STEPS,
                "the run took {MAX_STEPS} steps"
            );
            if self.hold_in_register && self.held.is_none() {
                let in_register = self.sched.ready().into_iter().any(|ready| {
                    self.sched
                        .task_name(ready.task)
                        .ends_with("quic.inbound_handshake.1")
                        && ready.at.op == Op::Watch
                        && ready
                            .at
                            .site
                            .is_some_and(|site| site.file().ends_with("zakura/handler.rs"))
                });
                if in_register {
                    self.held = Some("quic.inbound_handshake");
                }
            }
            let ready: Vec<_> = self
                .sched
                .ready()
                .into_iter()
                .filter(|ready| {
                    self.held.is_none_or(|held| {
                        let path = self.sched.task_name(ready.task);
                        !path.rsplit('/').next().unwrap_or(&path).starts_with(held)
                    })
                })
                .collect();
            if !ready.is_empty() {
                // A Vec's length fits in u64, and the choice is below it.
                let index = self.plan.borrow_mut().below(ready.len() as u64) as usize;
                let pick = ready[index];
                self.steps
                    .push(format!("{}:{}", self.sched.task_name(pick.task), pick.at));
                if let RunOutcome::Panicked(message) = self.sched.run(pick.task) {
                    panic!(
                        "task {} panicked: {message}",
                        self.sched.task_name(pick.task)
                    );
                }
                continue;
            }
            let deliverable = self.net.deliverable();
            if !deliverable.is_empty() {
                // As above.
                let index = self.plan.borrow_mut().below(deliverable.len() as u64) as usize;
                self.net
                    .deliver(deliverable[index])
                    .expect("a deliverable datagram delivers");
                continue;
            }
            let now_std = zakura_rt::time::Instant::now().into_std();
            let now = self.sched.now();
            let limit = self
                .net
                .time_limit()
                .map(|limit| sim_of(limit, now_std, now));
            let target = [
                self.sched.next_deadline(),
                self.net.next_eligible().map(|at| sim_of(at, now_std, now)),
            ]
            .into_iter()
            .flatten()
            .min()
            .map(|target| limit.map_or(target, |limit| target.min(limit)))
            // A timer due now fires only on an advance, so `now` is a target.
            .filter(|target| *target >= now && self.net.can_advance());
            match target {
                Some(target) if target.since_start() <= until => {
                    self.sched.advance_to(target);
                }
                _ => return,
            }
        }
    }

    /// The events the observer recorded so far.
    fn events(&self) -> Vec<TransportEvent> {
        self.recorder.events()
    }

    /// Stops the peer and the node, and runs every task to its end.
    fn finish(mut self) {
        self.held = None;
        self.go.cancel();
        self.stop.cancel();
        self.shutdown.cancel();
        let end = self.sched.now().since_start() + Duration::from_secs(60);
        self.run_until(end);
        let _ = self.sched.shutdown();
    }
}

/// The admitted header stream's connection and QUIC stream.
fn header_stream(events: &[TransportEvent]) -> (u64, u64) {
    events
        .iter()
        .find_map(|event| match event {
            TransportEvent::Admitted {
                conn_id,
                quic_stream,
                kind: ZAKURA_STREAM_HEADER_SYNC,
                ..
            } => Some((*conn_id, *quic_stream)),
            _ => None,
        })
        .expect("the header stream was admitted")
}

/// The reader stages of `events` on `quic_stream`.
fn reads(events: &[TransportEvent], stream: u64) -> Vec<TransportEvent> {
    events
        .iter()
        .filter(|event| {
            matches!(event,
                TransportEvent::HeaderRead { quic_stream, .. }
                | TransportEvent::FrameRead { quic_stream, .. } if *quic_stream == stream)
        })
        .cloned()
        .collect()
}

/// The close decisions in `events`.
fn closes(events: &[TransportEvent]) -> Vec<TransportEvent> {
    events
        .iter()
        .filter(|event| matches!(event, TransportEvent::CloseDecided { .. }))
        .cloned()
        .collect()
}

/// A frame header for `message_type` that declares `payload_len` bytes.
fn frame_header(message_type: u16, payload_len: u32) -> Vec<u8> {
    [
        &message_type.to_le_bytes()[..],
        &0u16.to_le_bytes(),
        &payload_len.to_le_bytes(),
    ]
    .concat()
}

/// The `Status` row's payload length.
fn status_len() -> u32 {
    let row = RegtestMessage::ROWS
        .iter()
        .find(|row| row.name == "status")
        .expect("the table has a status row");
    u32::try_from(row.payload.min()).expect("a row length fits u32")
}

/// A header cut after 5 bytes reaches neither reader stage, and a frame cut
/// mid-payload reaches only `HeaderRead`, at a point after all ready work
/// ran and before the reader's deadline for the missing bytes. Past that
/// deadline, under production timeouts, each closes with the reader's
/// neutral timeout cause.
///
/// This fails if `HeaderRead` moves before the header is read, or if a
/// partial frame gets a fault cause.
#[test]
fn partial_frames_reach_only_their_stage() {
    let status = status_len();
    let whole_header = frame_header(1, status);
    let cases = [
        ("a header cut after 5 bytes", whole_header[..5].to_vec(), 0),
        (
            "a frame cut mid-payload",
            [whole_header.clone(), vec![0; 4]].concat(),
            1,
        ),
    ];
    for (case, bytes, stages) in cases {
        let mut wire = Wire::start(
            11,
            Script {
                first: bytes,
                ..Script::default()
            },
        );
        // Ready work drains long before the reader's deadline, which is the
        // connection's idle timeout of almost 150 s.
        wire.run_until(Duration::from_secs(5));
        let events = wire.events();
        let (conn_id, quic_stream) = header_stream(&events);
        let expected = [TransportEvent::HeaderRead {
            conn_id,
            quic_stream,
            seq: 0,
            message_type: 1,
            flags: 0,
            payload_len: status,
        }];
        assert_eq!(reads(&events, quic_stream), expected[..stages], "{case}");
        assert_eq!(closes(&events), vec![], "{case}");

        wire.run_until(Duration::from_secs(160));
        let events = wire.events();
        assert_eq!(reads(&events, quic_stream), expected[..stages], "{case}");
        assert_eq!(
            closes(&events),
            vec![TransportEvent::CloseDecided {
                remote: peer_addr(),
                conn_id: Some(conn_id),
                cause: Some("ordered_read_timeout"),
            }],
            "{case}: the timeout is neutral"
        );
        wire.finish();
    }
}

/// The node decides to close when its reader rejects a header, while the
/// connection's teardown waits behind its held serving task, which runs the
/// peer's unregistration. `TornDown` follows only once it runs.
///
/// This fails if `CloseDecided` moves to teardown.
#[test]
fn close_is_observed_before_teardown() {
    let frame_cap = RegtestMessage::LAYOUT[0].frame_cap;
    let mut wire = Wire::start(
        13,
        Script {
            second: frame_header(1, frame_cap),
            ..Script::default()
        },
    );
    wire.run_until(Duration::from_secs(5));
    let (conn_id, quic_stream) = header_stream(&wire.events());

    wire.held = Some("quic.inbound_handshake");
    wire.go.cancel();
    wire.run_until(Duration::from_secs(10));
    let events = wire.events();
    assert_eq!(
        reads(&events, quic_stream),
        [TransportEvent::HeaderRead {
            conn_id,
            quic_stream,
            seq: 0,
            message_type: 1,
            flags: 0,
            payload_len: frame_cap,
        }]
    );
    assert_eq!(
        closes(&events),
        vec![TransportEvent::CloseDecided {
            remote: peer_addr(),
            conn_id: Some(conn_id),
            cause: Some("ordered_read_rejected"),
        }],
        "the close is decided while teardown is held"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, TransportEvent::TornDown { .. })),
        "teardown waits for its task"
    );

    wire.held = None;
    wire.run_until(Duration::from_secs(15));
    let events = wire.events();
    let closed = events
        .iter()
        .position(|event| matches!(event, TransportEvent::CloseDecided { .. }))
        .expect("the close was decided");
    let torn_down = events
        .iter()
        .position(|event| *event == TransportEvent::TornDown { conn_id })
        .expect("the released task tears the connection down");
    assert!(closed < torn_down);
    wire.finish();
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
            let (quic, handler, limits) =
                bind_node(&net, service, Some(Arc::new(Recorder::default())));
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
            let (quic, handler, _limits) = bind_node(&net, service, None);
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

/// The handshake, registration, a refused stream and the header stream's
/// admission are each observed, keyed as the identity rules say: the
/// handshake by the remote address, the rest by the registered connection.
#[test]
fn a_connection_is_observed_from_handshake_to_admission() {
    // Kind 4 is discovery, which this node doesn't register.
    let mut wire = Wire::start(
        19,
        Script {
            other_stream: Some((4, 2)),
            ..Script::default()
        },
    );
    wire.run_until(Duration::from_secs(5));
    let events = wire.events();
    let (conn_id, quic_stream) = header_stream(&events);
    let limits = ZakuraLocalLimits::from_config(&regtest_config());
    let TransportEvent::StreamRefused {
        quic_stream: refused,
        ..
    } = events[2]
    else {
        panic!("the other stream is refused before admission: {events:?}");
    };
    assert_ne!(refused, quic_stream);
    assert_eq!(
        events,
        vec![
            TransportEvent::Negotiated {
                remote: peer_addr(),
                local: ZAKURA_CAP_HEADER_SYNC,
                offered: ZAKURA_CAP_HEADER_SYNC,
                negotiated: ZAKURA_CAP_HEADER_SYNC,
            },
            TransportEvent::Registered {
                conn_id,
                remote: peer_addr(),
            },
            TransportEvent::StreamRefused {
                conn_id,
                quic_stream: refused,
                kind: 4,
                version: 2,
                reason: "unknown_kind",
            },
            TransportEvent::Admitted {
                conn_id,
                quic_stream,
                kind: ZAKURA_STREAM_HEADER_SYNC,
                version: ZAKURA_HEADER_SYNC_STREAM_VERSION,
                session_id: match events[3] {
                    TransportEvent::Admitted { session_id, .. } => session_id,
                    _ => panic!("the header stream is admitted last: {events:?}"),
                },
                negotiated_limits: limits.clamp(&limits.initial_limits()),
                inbound_depth: 4,
                outbound_depth: usize::from(limits.max_inbound_queue_depth),
            },
        ]
    );
    wire.finish();
}

/// The peer's identity on the node's supervisor.
fn wire_peer() -> ZakuraPeerId {
    let node = SeededInputs::node_key(SEED, 1).public();
    ZakuraPeerId::new(node.as_bytes().to_vec()).expect("a node id is a valid peer id")
}

/// A supervisor disconnect with no recorded cause is decided when the
/// supervisor cancels the connection, while the connection's serving task,
/// which would otherwise notice it first, is held. Teardown follows once it
/// runs.
///
/// This fails if the decision waits for the serving task.
#[test]
fn a_cancellation_is_decided_when_it_happens() {
    let mut wire = Wire::start(
        23,
        Script {
            disconnect: true,
            ..Script::default()
        },
    );
    wire.run_until(Duration::from_secs(5));
    let (conn_id, _) = header_stream(&wire.events());

    wire.held = Some("quic.inbound_handshake");
    wire.go.cancel();
    wire.run_until(Duration::from_secs(10));
    let events = wire.events();
    assert_eq!(
        closes(&events),
        vec![TransportEvent::CloseDecided {
            remote: peer_addr(),
            conn_id: Some(conn_id),
            cause: None,
        }],
        "the cancellation is decided while the serving task is held"
    );
    assert!(!events
        .iter()
        .any(|event| matches!(event, TransportEvent::TornDown { .. })));

    wire.held = None;
    wire.run_until(Duration::from_secs(15));
    let events = wire.events();
    assert_eq!(closes(&events).len(), 1, "the decision is reported once");
    assert!(events.contains(&TransportEvent::TornDown { conn_id }));
    wire.finish();
}

/// A connection whose stream worker outlives the drain timeout reports no
/// `TornDown`, though its close is decided. The held worker is the header
/// stream's service session, which the node aborts but can't join.
///
/// This fails if a timed-out drain reports `TornDown`.
#[test]
fn a_timed_out_drain_reports_no_teardown() {
    let mut wire = Wire::start(
        29,
        Script {
            disconnect: true,
            ..Script::default()
        },
    );
    wire.run_until(Duration::from_secs(5));
    let (conn_id, _) = header_stream(&wire.events());

    wire.held = Some("service_session");
    wire.go.cancel();
    // The drain gives up after 1 s.
    wire.run_until(Duration::from_secs(15));
    let events = wire.events();
    assert_eq!(
        closes(&events),
        vec![TransportEvent::CloseDecided {
            remote: peer_addr(),
            conn_id: Some(conn_id),
            cause: None,
        }]
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, TransportEvent::TornDown { .. })),
        "a held worker leaves the drain incomplete: {events:?}"
    );

    wire.held = None;
    wire.run_until(Duration::from_secs(20));
    assert!(!wire
        .events()
        .iter()
        .any(|event| matches!(event, TransportEvent::TornDown { .. })));
    wire.finish();
}

/// Installing the transport observer adds no scheduling point: one plan takes
/// the same steps with and without it, through the handshake, admission,
/// a whole frame and the regulation checks of a page header.
///
/// This fails if attaching a tap takes a simulator lock.
#[test]
fn the_observer_adds_no_scheduling_point() {
    let status = status_len();
    let script = Script {
        first: [
            frame_header(1, status),
            vec![0; usize::try_from(status).expect("u32 fits usize")],
            frame_header(3, 300),
        ]
        .concat(),
        ..Script::default()
    };
    let mut steps = Vec::new();
    for observed in [true, false] {
        let mut wire = Wire::start_observed(29, script.clone(), observed);
        wire.run_until(Duration::from_secs(5));
        assert_eq!(
            wire.events()
                .iter()
                .any(|event| matches!(event, TransportEvent::Regulated { .. })),
            observed,
            "only the observed run reports, and it reached the regulation checks"
        );
        steps.push(std::mem::take(&mut wire.steps));
        wire.finish();
    }
    assert_eq!(steps[0], steps[1]);
}

/// A peer that answers the node's dial: it acks the node's hello with the
/// nonce echoed and header sync accepted, accepts the header stream the node
/// opens, and holds the connection until it closes.
struct Responder;

impl Acceptor for Responder {
    /// Accepts every attempt.
    fn admit(&self, _incoming: &IncomingInfo) -> Admit {
        Admit::Accept
    }

    /// The node's ALPN.
    fn alpns(&self) -> Vec<Vec<u8>> {
        vec![P2P_V2_ALPN.to_vec()]
    }

    /// Runs the responder's side of the control handshake.
    fn handle(&self, conn: Conn) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let Ok((mut send, mut recv)) = conn.accept_bi().await else {
                return;
            };
            let mut len = [0; 4];
            recv.read_exact(&mut len)
                .await
                .expect("the hello's length arrives");
            let mut hello =
                vec![0; usize::try_from(u32::from_le_bytes(len)).expect("u32 fits usize")];
            recv.read_exact(&mut hello)
                .await
                .expect("the hello arrives");
            let hello = ZakuraControlHello::decode(&hello).expect("the hello decodes");
            let ack = ZakuraControlAck {
                magic: CONTROL_ACK_MAGIC,
                control_version: CONTROL_VERSION,
                selected_zakura_protocol: hello.selected_zakura_protocol,
                peer_nonce: [9; 32],
                remote_peer_nonce: hello.peer_nonce,
                accepted_capabilities: hello.capabilities & ZAKURA_CAP_HEADER_SYNC,
                accepted_channels: 0,
                accepted_limits: hello.initial_limits,
            }
            .encode()
            .expect("the ack encodes");
            let len = u32::try_from(ack.len()).expect("an ack fits u32");
            send.write_all(&len.to_le_bytes())
                .await
                .expect("the ack's length is written");
            send.write_all(&ack).await.expect("the ack is written");
            let _ = send.finish();
            // The node opens the header stream. Hold it until the end.
            let _header_stream = conn.accept_bi().await;
            conn.closed().await;
        })
    }
}

/// Dropping an admitted `dial` decides its close at once, through the
/// connection's cleanup guard, though no task saw the token cancelled. Endpoint
/// shutdown later reports nothing more for it.
///
/// This fails if the guard's cancellation skips the observer.
#[test]
fn a_dropped_dial_is_decided_at_once() {
    let plan = Rc::new(RefCell::new(Plan(31)));
    let mut sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    {
        let plan = Rc::clone(&plan);
        sched.set_draws(move |draw| plan.borrow_mut().below(draw.bound));
    }
    let net = SimNet::new(LinkModel::default());
    let recorder = Arc::new(Recorder::default());
    let shutdown = CancellationToken::new();
    let (go, stop) = (CancellationToken::new(), CancellationToken::new());
    let (handle, actions, reactor) =
        spawn_header_sync_reactor(startup(shutdown.clone())).expect("the reactor starts");
    sched.spawn_root("reactor", async move {
        reactor.await.expect("the reactor ends on shutdown");
    });
    let service: Arc<dyn Service> = Arc::new(HeaderSyncService::new(handle));
    {
        let (net, recorder, go, stop) = (net.clone(), recorder.clone(), go.clone(), stop.clone());
        sched.spawn_root("node", async move {
            let _actions = actions;
            let (quic, handler, limits) = bind_node(&net, service, Some(recorder));
            let peer = QuicEndpoint::bind_with(
                SeededInputs::node_key(SEED, 1),
                &bind_config(peer_addr()),
                &QuicConfig::default(),
                net,
                SeededInputs::new(SEED, 1).endpoint_inputs(),
            )
            .expect("the peer binds");
            peer.serve(Responder).expect("the peer serves");
            let endpoint = ZakuraEndpoint::new(quic, handler);
            let target = NodeAddr::with_addrs(peer.local_id(), [peer_addr()]);
            zakura_rt::select! {
                result = endpoint.dial(target, &limits) => {
                    panic!("the dial ended before the test dropped it: {result:?}");
                }
                () = go.cancelled() => {}
            }
            stop.cancelled().await;
            endpoint.shutdown().await;
            peer.shutdown().await;
        });
    }
    let mut wire = Wire {
        sched,
        plan,
        net,
        recorder,
        stop,
        go,
        shutdown,
        held: None,
        hold_in_register: false,
        steps: Vec::new(),
    };
    wire.run_until(Duration::from_secs(5));
    let (conn_id, _) = header_stream(&wire.events());
    assert_eq!(closes(&wire.events()), vec![]);

    wire.go.cancel();
    wire.run_until(Duration::from_secs(6));
    let decided = vec![TransportEvent::CloseDecided {
        remote: peer_addr(),
        conn_id: Some(conn_id),
        cause: None,
    }];
    assert_eq!(
        closes(&wire.events()),
        decided,
        "dropping the dial decides it"
    );

    wire.stop.cancel();
    wire.run_until(Duration::from_secs(20));
    assert_eq!(closes(&wire.events()), decided, "shutdown adds nothing");
    wire.finish();
}

/// An endpoint shutdown while the serving task is paused inside `register`,
/// after the supervisor published the connection and before `register`
/// returned, decides the close at once, keyed by the remote address, since no
/// `conn_id` has been reported yet. `Registered` follows once the task runs,
/// and the decision isn't repeated.
///
/// This fails if the connection token's cancellation hook isn't attached
/// before `register`.
#[test]
fn a_shutdown_during_registration_is_decided_at_once() {
    let mut wire = Wire::start(
        37,
        Script {
            shutdown: true,
            ..Script::default()
        },
    );
    wire.hold_in_register = true;
    wire.run_until(Duration::from_secs(5));
    assert!(wire.held.is_some(), "the serving task reached registration");
    assert!(
        !wire
            .events()
            .iter()
            .any(|event| matches!(event, TransportEvent::Registered { .. })),
        "registration hasn't returned"
    );

    wire.go.cancel();
    wire.run_until(Duration::from_secs(10));
    let decided = vec![TransportEvent::CloseDecided {
        remote: peer_addr(),
        conn_id: None,
        cause: None,
    }];
    assert_eq!(
        closes(&wire.events()),
        decided,
        "the shutdown is decided while registration is held"
    );

    wire.held = None;
    wire.hold_in_register = false;
    wire.run_until(Duration::from_secs(20));
    let events = wire.events();
    assert!(events.iter().any(
        |event| matches!(event, TransportEvent::Registered { remote, .. } if *remote == peer_addr())
    ));
    assert_eq!(closes(&events), decided, "the decision is reported once");
    wire.finish();
}
