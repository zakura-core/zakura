//! M3: the real `zakura-quic` endpoint and connection tasks replay a QUIC
//! handshake byte for byte on the deterministic scheduler. These only build
//! with `RUSTFLAGS="--cfg zakura_sim"`, in a separate `CARGO_TARGET_DIR`.
//!
//! Each run binds two endpoints on a [`SimNet`] with [`SeededInputs`], has the
//! server answer the first Initial with a Retry, and echoes one stream. No
//! tokio runtime exists: the scheduler runs every task, and a small seeded
//! plan picks which ready task runs, which datagram arrives or is lost, and
//! when time moves. The transcript digest is SHA-256 over every datagram in
//! delivery order, with each loss marked.
#![cfg(zakura_sim)]

use std::{
    cell::RefCell,
    collections::BTreeMap,
    future::Future,
    io::{self, Write as _},
    net::{Ipv4Addr, SocketAddr},
    process::Command,
    rc::Rc,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::future::BoxFuture;
use sha2::{Digest as _, Sha256};
use zakura_quic::{
    sim::{DatagramId, Link, LinkModel, NetEvent, SeededInputs, SimNet, SimSocket, Source},
    Acceptor, Admit, BoundSocket, Conn, IncomingInfo, NodeAddr, QuicBindConfig, QuicConfig,
    QuicEndpoint, RecvMeta, SocketFactory, Transmit, UdpIo,
};
use zakura_scheduler::{RunOutcome, Scheduler, SchedulerConfig, SimInstant, Status};

/// A run that takes more steps than this is stuck.
const MAX_STEPS: usize = 200_000;

/// The ALPN both endpoints speak.
const ALPN: &[u8] = b"p2p-v2/3";

/// What the client sends and the server echoes.
const PAYLOAD: &[u8] = b"m3 replay payload!";

/// The environment variable that makes [`replay_child`] print a digest.
const CHILD_ENV: &str = "ZAKURA_QUIC_REPLAY_CHILD";

/// The server's address.
fn server_addr() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::new(10, 0, 0, 1), 4000))
}

/// The client's address.
fn client_addr() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::new(10, 0, 0, 2), 4000))
}

/// A seeded stream of choices (SplitMix64).
struct Plan(u64);

impl Plan {
    /// The next 64 bits.
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

/// Everything one run is built from.
#[derive(Clone, Debug)]
struct Scenario {
    /// The `SeededInputs` seed for both nodes.
    seed: [u8; 32],
    /// Seeds the plan of task, network and draw choices.
    plan: u64,
    /// UTC at virtual time zero. `None` keeps `zakura-rt`'s default.
    utc_origin: Option<SystemTime>,
    /// A source both nodes leave unset (M3.3).
    omit: Option<Source>,
    /// Every link's model, unless overridden below.
    links: LinkModel,
    /// The client-to-server link's model, if it differs.
    to_server: Option<LinkModel>,
}

impl Scenario {
    /// Seed 0x5a, plan 1, the default origin, and 1 to 5 ms FIFO links.
    fn base() -> Self {
        Self {
            seed: [0x5a; 32],
            plan: 1,
            utc_origin: None,
            omit: None,
            links: LinkModel {
                min_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(5),
                reorder: false,
                loss_budget: 0,
            },
            to_server: None,
        }
    }
}

/// What a run lets a test compare.
#[derive(Debug, PartialEq, Eq)]
struct Observations {
    /// The transcript digest (see [`transcript_digest`]), as hex.
    digest: String,
    /// Each step: `task:point`, `deliver`, `drop` or `advance`, in order.
    steps: Vec<String>,
    /// The timers each time advance fired, in order.
    timers: Vec<String>,
    /// Each random draw's kind and bound.
    draws: Vec<String>,
    /// How the scenario ended: `echoed`, or the connect error.
    outcome: String,
    /// What the scheduler cleaned up at the end.
    cleanup: String,
}

/// The sends one run made, keyed as `SimNet` keys them.
#[derive(Default)]
struct Recording {
    /// Each link's sends so far, which is the next send's sequence.
    sent: BTreeMap<Link, u64>,
    /// The bytes of each datagram sent.
    bytes: BTreeMap<DatagramId, Vec<u8>>,
}

/// A [`SimNet`] factory that records every datagram its sockets send.
#[derive(Clone)]
struct Recorder {
    net: SimNet,
    recording: Arc<Mutex<Recording>>,
}

impl SocketFactory for Recorder {
    type Socket = RecordingSocket;

    /// Binds on the network and wraps the socket to record its sends.
    fn bind(&self, addr: SocketAddr) -> io::Result<BoundSocket<RecordingSocket>> {
        let bound = <SimNet as SocketFactory>::bind(&self.net, addr)?;
        Ok(BoundSocket {
            io: RecordingSocket {
                inner: bound.io,
                local: bound.local_addr,
                recording: self.recording.clone(),
            },
            local_addr: bound.local_addr,
            buffers: bound.buffers,
            inode: bound.inode,
        })
    }
}

/// A [`SimSocket`] that records the bytes of each send it accepts.
#[derive(Debug)]
struct RecordingSocket {
    inner: SimSocket,
    local: SocketAddr,
    recording: Arc<Mutex<Recording>>,
}

impl std::fmt::Debug for Recording {
    /// Counts the datagrams recorded.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Recording({} datagrams)", self.bytes.len())
    }
}

impl UdpIo for RecordingSocket {
    /// The simulated socket's readiness.
    fn readable(&self) -> impl Future<Output = io::Result<()>> + Send {
        self.inner.readable()
    }

    /// The simulated socket's readiness.
    fn writable(&self) -> impl Future<Output = io::Result<()>> + Send {
        self.inner.writable()
    }

    /// Receives from the simulated socket.
    fn try_recv(&self, buf: &mut [u8]) -> io::Result<Option<RecvMeta>> {
        self.inner.try_recv(buf)
    }

    /// Sends through the simulated socket and records the datagram under
    /// the identity `SimNet` gives it: its link and that link's next
    /// sequence.
    fn try_send(&self, transmit: &Transmit, contents: &[u8]) -> io::Result<()> {
        assert!(
            transmit.segment_size.is_none(),
            "a simulated socket sends one datagram per transmit"
        );
        self.inner.try_send(transmit, contents)?;
        let link = Link {
            src: self.local,
            dst: transmit.destination,
        };
        let mut recording = self
            .recording
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let seq = recording.sent.entry(link).or_default();
        let id = DatagramId { link, seq: *seq };
        *seq += 1;
        recording.bytes.insert(id, contents.to_vec());
        Ok(())
    }

    /// No GSO, as the simulated socket.
    fn gso(&self) -> bool {
        self.inner.gso()
    }

    /// No MTU discovery, as the simulated socket.
    fn mtud_allowed(&self) -> bool {
        self.inner.mtud_allowed()
    }
}

/// Retries every unvalidated attempt and echoes one stream per connection.
struct EchoAcceptor;

impl Acceptor for EchoAcceptor {
    /// A Retry first, then accept once the address is validated.
    fn admit(&self, incoming: &IncomingInfo) -> Admit {
        if incoming.validated {
            Admit::Accept
        } else {
            Admit::Retry
        }
    }

    /// The one test ALPN.
    fn alpns(&self) -> Vec<Vec<u8>> {
        vec![ALPN.to_vec()]
    }

    /// Echoes the first stream, then waits for the client to close.
    fn handle(&self, conn: Conn) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            if let Ok((mut send, mut recv)) = conn.accept_bi().await {
                if let Ok(data) = recv.read_to_end(PAYLOAD.len() + 1).await {
                    let _ = send.write_all(&data).await;
                    let _ = send.finish();
                }
            }
            conn.closed().await;
        })
    }
}

/// The transport config for both nodes. The 60 s handshake timeout leaves
/// room for M3.5's slow links.
fn quic_config() -> QuicConfig {
    QuicConfig {
        handshake_timeout_secs: Some(60),
        ..QuicConfig::default()
    }
}

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

/// Node `node`'s endpoint inputs for `scenario`.
fn inputs(scenario: &Scenario, node: u32) -> zakura_quic::EndpointInputs {
    let seeded = SeededInputs::new(scenario.seed, node);
    match scenario.omit {
        Some(source) => seeded.endpoint_inputs_without(source),
        None => seeded.endpoint_inputs(),
    }
}

/// Binds both nodes, has the client dial through a Retry and echo
/// [`PAYLOAD`], then shuts both down. Returns how it ended.
async fn handshake_and_echo(scenario: Scenario, factory: Recorder) -> String {
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "the scenario runs on the scheduler, with no tokio runtime"
    );
    let server = QuicEndpoint::bind_with(
        SeededInputs::node_key(scenario.seed, 0),
        &bind_config(server_addr()),
        &quic_config(),
        factory.clone(),
        inputs(&scenario, 0),
    )
    .expect("the server binds");
    server.serve(EchoAcceptor).expect("the server serves");
    let client = QuicEndpoint::bind_with(
        SeededInputs::node_key(scenario.seed, 1),
        &bind_config(client_addr()),
        &quic_config(),
        factory,
        inputs(&scenario, 1),
    )
    .expect("the client binds");

    let target = NodeAddr::with_addrs(server.local_id(), [server_addr()]);
    let outcome = match client.connect(target, &[ALPN]).await {
        Ok(conn) => {
            let (mut send, mut recv) = conn.open_bi().await.expect("the client opens a stream");
            send.write_all(PAYLOAD).await.expect("the payload is sent");
            send.finish().expect("the stream finishes");
            let echoed = recv
                .read_to_end(PAYLOAD.len() + 1)
                .await
                .expect("the echo arrives");
            assert_eq!(echoed, PAYLOAD);
            conn.close(zakura_quic::VarInt::from_u32(0), b"done");
            "echoed".to_owned()
        }
        Err(error) => format!("connect failed: {error}"),
    };
    client.shutdown().await;
    server.shutdown().await;
    outcome
}

/// Converts a runtime instant to scheduler time, given one moment read
/// from both clocks.
fn sim_of(instant: std::time::Instant, now_std: std::time::Instant, now: SimInstant) -> SimInstant {
    SimInstant::from_start(now.since_start() + instant.saturating_duration_since(now_std))
}

/// Runs `scenario` to the end and returns what it observed, with the
/// network's events and every datagram's bytes.
fn run_with_transcript(
    scenario: &Scenario,
) -> (Observations, Vec<NetEvent>, BTreeMap<DatagramId, Vec<u8>>) {
    let plan = Rc::new(RefCell::new(Plan(scenario.plan)));
    let draws = Rc::new(RefCell::new(Vec::new()));
    let config = SchedulerConfig {
        utc_origin: scenario.utc_origin,
        ..SchedulerConfig::default()
    };
    let mut sched = Scheduler::new(config, SimInstant::ZERO);
    {
        let (plan, draws) = (Rc::clone(&plan), Rc::clone(&draws));
        sched.set_draws(move |draw| {
            draws
                .borrow_mut()
                .push(format!("{:?}/{}", draw.kind, draw.bound));
            plan.borrow_mut().below(draw.bound)
        });
    }

    let net = SimNet::new(scenario.links);
    if let Some(model) = scenario.to_server {
        net.set_link(
            Link {
                src: client_addr(),
                dst: server_addr(),
            },
            model,
        );
    }
    let factory = Recorder {
        net: net.clone(),
        recording: Arc::default(),
    };
    let recording = factory.recording.clone();
    let outcome = Rc::new(RefCell::new(None));
    let root = {
        let (scenario, outcome) = (scenario.clone(), Rc::clone(&outcome));
        sched.spawn_root("scenario", async move {
            let ended = handshake_and_echo(scenario, factory).await;
            *outcome.borrow_mut() = Some(ended);
        })
    };

    let mut steps = Vec::new();
    let mut timers = Vec::new();
    loop {
        assert!(steps.len() < MAX_STEPS, "the run took {MAX_STEPS} steps");
        if sched.status() == Status::Running {
            let ready = sched.ready();
            let index = plan.borrow_mut().below(ready.len() as u64); // a Vec's length fits in u64
            let pick = ready[index as usize]; // below `ready.len()`, a usize
            steps.push(format!("{}:{}", sched.task_name(pick.task), pick.at));
            if let RunOutcome::Panicked(message) = sched.run(pick.task) {
                panic!("task {} panicked: {message}", sched.task_name(pick.task));
            }
            continue;
        }
        if root.is_finished() {
            break;
        }

        // Nothing is ready: deliver, lose or wait. Options are ordered by
        // identity, deliveries and losses first, then time.
        let deliverable = net.deliverable();
        let droppable = net.droppable();
        let now_std = zakura_rt::time::Instant::now().into_std();
        let now = sched.now();
        let limit = net.time_limit().map(|limit| sim_of(limit, now_std, now));
        let target = [
            sched.next_deadline(),
            net.next_eligible().map(|at| sim_of(at, now_std, now)),
        ]
        .into_iter()
        .flatten()
        .min()
        .map(|target| limit.map_or(target, |limit| target.min(limit)))
        // A timer due now fires only on an advance, so `now` is a target.
        .filter(|target| *target >= now && net.can_advance());
        let options = deliverable.len() + droppable.len() + usize::from(target.is_some());
        assert!(options > 0, "the run stalled: {:?}", sched.status());
        // `options` fits in u64, and the choice is below it, so in usize.
        let choice = plan.borrow_mut().below(options as u64) as usize;
        if let Some(id) = deliverable.get(choice) {
            net.deliver(*id).expect("a deliverable datagram delivers");
            steps.push(format!("deliver {id:?}"));
        } else if let Some(id) = droppable.get(choice - deliverable.len()) {
            net.drop_datagram(*id).expect("a droppable datagram drops");
            steps.push(format!("drop {id:?}"));
        } else {
            let target = target.expect("the last option is advancing time");
            let fired = sched.advance_to(target);
            steps.push(format!("advance {:?}", target.since_start()));
            timers.push(format!("{fired:?}"));
        }
    }
    let cleanup = format!("{:?}", sched.shutdown());

    let recording = recording.lock().unwrap_or_else(PoisonError::into_inner);
    let events = net.events();
    let outcome = outcome
        .borrow_mut()
        .take()
        .expect("the scenario task finished");
    let observations = Observations {
        digest: transcript_digest(&events, &recording.bytes),
        steps,
        timers,
        draws: draws.take(),
        outcome,
        cleanup,
    };
    (observations, events, recording.bytes.clone())
}

/// Runs `scenario` to the end and returns what it observed.
fn run(scenario: &Scenario) -> Observations {
    run_with_transcript(scenario).0
}

/// SHA-256 over the network's events in order, as hex: each send with its
/// identity, length and bytes, then each delivery and each loss. Every
/// byte a socket sent counts, delivered, lost or still in flight.
fn transcript_digest(events: &[NetEvent], bytes: &BTreeMap<DatagramId, Vec<u8>>) -> String {
    let mut digest = Sha256::new();
    for event in events {
        match event {
            NetEvent::Sent { id, len } => {
                let sent = &bytes[id];
                assert_eq!(
                    sent.len(),
                    *len,
                    "the recorder keys datagrams as SimNet does"
                );
                digest.update(format!("send {id:?} {len}").as_bytes());
                digest.update(sent);
            }
            NetEvent::Delivered { id } => digest.update(format!("deliver {id:?}").as_bytes()),
            NetEvent::Dropped { id, reason } => {
                digest.update(format!("drop {id:?} {reason:?}").as_bytes());
            }
            NetEvent::Bound { .. } | NetEvent::Unbound { .. } => {}
        }
    }
    hex::encode(digest.finalize())
}

/// M3.1 helper: run as a child process, print the base scenario's digest.
/// The parent test sets [`CHILD_ENV`], and a normal test run skips it.
#[test]
fn replay_child() {
    if std::env::var_os(CHILD_ENV).is_none() {
        return;
    }
    let digest = run(&Scenario::base()).digest;
    // Straight to stdout, past libtest's output capture.
    writeln!(io::stdout(), "digest={digest}").expect("stdout takes the digest");
}

/// Runs [`replay_child`] in a fresh process and returns its digest.
fn digest_from_fresh_process() -> String {
    let output = Command::new(std::env::current_exe().expect("the test binary's path"))
        .args(["replay_child", "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, "1")
        .output()
        .expect("the child test runs");
    assert!(output.status.success(), "{output:?}");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        // libtest may print the test's name on the same line.
        .find_map(|line| {
            let (_, digest) = line.split_once("digest=")?;
            Some(digest.chars().take(64).collect())
        })
        .unwrap_or_else(|| panic!("the child printed no digest: {output:?}"))
}

/// M3.1: the same inputs and plan send byte-identical datagrams through a
/// Retry handshake and an echoed stream, twice in this process and once in
/// each of two fresh processes.
#[test]
fn a_retry_handshake_replays_byte_for_byte() {
    let first = run(&Scenario::base());
    assert_eq!(first.outcome, "echoed");
    assert!(
        first
            .steps
            .iter()
            .any(|step| step.contains("/quic.connection.")),
        "the real connection tasks ran: {:?}",
        first.steps
    );
    assert_eq!(
        run(&Scenario::base()),
        first,
        "a second run in this process"
    );
    for _ in 0..2 {
        assert_eq!(digest_from_fresh_process(), first.digest, "a fresh process");
    }
}

/// M3.2: another seed sends different bytes, and so does another UTC
/// origin, since Retry tokens carry their issue time. Two seconds of host
/// time between runs change nothing.
#[test]
fn the_seed_and_utc_origin_change_the_transcript_and_host_time_does_not() {
    let base = run(&Scenario::base());
    let other_seed = run(&Scenario {
        seed: [0xa5; 32],
        ..Scenario::base()
    });
    assert_eq!(other_seed.outcome, "echoed");
    assert_ne!(other_seed.digest, base.digest, "another seed");

    let later_origin = run(&Scenario {
        utc_origin: Some(UNIX_EPOCH + Duration::from_secs(1_800_000_000)),
        ..Scenario::base()
    });
    assert_eq!(later_origin.outcome, "echoed");
    assert_ne!(later_origin.digest, base.digest, "another UTC origin");

    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        run(&Scenario::base()).digest,
        base.digest,
        "2 s of host time later"
    );
}

/// M3.3: leaving out any one seeded source makes two runs differ, because
/// that source falls back to the OS random source.
#[test]
fn every_seeded_source_reaches_the_transcript() {
    for source in Source::ALL {
        let scenario = Scenario {
            omit: Some(source),
            ..Scenario::base()
        };
        let first = run(&scenario);
        assert_eq!(first.outcome, "echoed", "{source:?}");
        assert_ne!(
            run(&scenario).digest,
            first.digest,
            "{source:?} left out, yet two runs matched"
        );
    }
}

/// The base scenario on reordering 1 to 50 ms links that may lose up to
/// three datagrams each, under `plan`.
fn lossy(plan: u64) -> Scenario {
    Scenario {
        plan,
        links: LinkModel {
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(50),
            reorder: true,
            loss_budget: 3,
        },
        ..Scenario::base()
    }
}

/// The digest covers the bytes of datagrams that were lost, not only the
/// ones delivered: changing one byte of a dropped datagram changes it.
#[test]
fn the_digest_covers_lost_datagrams_bytes() {
    let (observed, events, mut bytes) = (1..=4)
        .map(|plan| run_with_transcript(&lossy(plan)))
        .find(|(_, events, _)| {
            events
                .iter()
                .any(|event| matches!(event, NetEvent::Dropped { .. }))
        })
        .expect("some plan loses a datagram");
    let dropped = events
        .iter()
        .find_map(|event| match event {
            NetEvent::Dropped { id, .. } => Some(*id),
            _ => None,
        })
        .expect("the run lost a datagram");
    bytes.get_mut(&dropped).expect("the lost datagram was sent")[0] ^= 1;
    assert_ne!(transcript_digest(&events, &bytes), observed.digest);
}

/// M3.4: with reordering and losses, the handshake and echo still complete,
/// and the same plan repeats the same bytes and task events. Plans differ
/// in what they deliver or lose.
#[test]
fn loss_and_reordering_replay_with_the_same_plan() {
    let mut digests = Vec::new();
    let mut lost = false;
    for plan in 1..=4 {
        let first = run(&lossy(plan));
        assert_eq!(first.outcome, "echoed", "plan {plan}");
        assert_eq!(run(&lossy(plan)), first, "plan {plan} again");
        lost |= first.steps.iter().any(|step| step.starts_with("drop"));
        digests.push(first.digest);
    }
    assert!(lost, "some plan loses a datagram");
    digests.sort();
    digests.dedup();
    assert!(digests.len() > 1, "plans change the transcript");
}

/// M3.5: Retry tokens follow scenario UTC, which moves with virtual time.
/// With a fixed client-to-server delay, the token returns that long after
/// the server issued it: 14 s is within the 15 s lifetime, and 16 s isn't,
/// though each run takes milliseconds of host time.
#[test]
fn retry_tokens_expire_on_virtual_time() {
    let delayed = |secs| Scenario {
        to_server: Some(LinkModel {
            min_delay: Duration::from_secs(secs),
            max_delay: Duration::from_secs(secs),
            reorder: false,
            loss_budget: 0,
        }),
        ..Scenario::base()
    };
    assert_eq!(run(&delayed(14)).outcome, "echoed");
    let late = run(&delayed(16));
    assert!(
        late.outcome.starts_with("connect failed"),
        "a token 16 s old: {}",
        late.outcome
    );
}

/// M3.7: the real driver runs with no tokio runtime, and an unrelated run in
/// between doesn't change a run: plan A, then B, then A again gives A's
/// task identities, draws, fired timers and bytes both times.
#[test]
fn a_b_a_runs_keep_their_identities_decisions_and_bytes() {
    let a = Scenario::base();
    let b = Scenario {
        plan: 2,
        seed: [0x33; 32],
        ..Scenario::base()
    };
    let first = run(&a);
    assert!(!first.timers.is_empty(), "the run fired timers");
    let other = run(&b);
    assert_eq!(run(&a), first);
    assert_ne!(other.steps, first.steps, "plan B takes other steps");
}

/// SOCK-9 under `bind_with`: a fatal receive error rebinds through the
/// injected factory, on the scheduler, and the connection survives it.
#[test]
fn a_rebind_goes_through_the_injected_factory() {
    let net = SimNet::new(Scenario::base().links);
    let factory = Recorder {
        net: net.clone(),
        recording: Arc::default(),
    };
    let mut sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
    let done = Rc::new(RefCell::new(false));
    let root = {
        let (net, done) = (net.clone(), Rc::clone(&done));
        sched.spawn_root("scenario", async move {
            let scenario = Scenario::base();
            let server = QuicEndpoint::bind_with(
                SeededInputs::node_key(scenario.seed, 0),
                &bind_config(server_addr()),
                &quic_config(),
                factory.clone(),
                inputs(&scenario, 0),
            )
            .unwrap();
            server.serve(EchoAcceptor).unwrap();
            let client = QuicEndpoint::bind_with(
                SeededInputs::node_key(scenario.seed, 1),
                &bind_config(client_addr()),
                &quic_config(),
                factory,
                inputs(&scenario, 1),
            )
            .unwrap();
            let conn = client
                .connect(
                    NodeAddr::with_addrs(server.local_id(), [server_addr()]),
                    &[ALPN],
                )
                .await
                .unwrap();
            assert!(net.fail_next_recv(server_addr(), io::ErrorKind::Other));
            while server.socket_stats()[0].rebinds == 0 {
                zakura_rt::time::sleep(Duration::from_millis(1)).await;
            }
            let (mut send, mut recv) = conn.open_bi().await.unwrap();
            send.write_all(PAYLOAD).await.unwrap();
            send.finish().unwrap();
            assert_eq!(recv.read_to_end(PAYLOAD.len() + 1).await.unwrap(), PAYLOAD);
            client.shutdown().await;
            server.shutdown().await;
            *done.borrow_mut() = true;
        })
    };
    let mut steps = 0;
    while !root.is_finished() {
        steps += 1;
        assert!(steps < MAX_STEPS, "the rebind run is stuck");
        if sched.status() == Status::Running {
            let task = sched.ready()[0].task;
            sched.run(task);
        } else if let Some(id) = net.deliverable().first() {
            net.deliver(*id).unwrap();
        } else {
            let now_std = zakura_rt::time::Instant::now().into_std();
            let now = sched.now();
            let next = [
                sched.next_deadline(),
                net.next_eligible().map(|at| sim_of(at, now_std, now)),
            ]
            .into_iter()
            .flatten()
            .min()
            .expect("the run waits on time or the network");
            sched.advance_to(next);
        }
    }
    let _ = sched.shutdown();
    assert!(*done.borrow());
    let rebind = [
        NetEvent::Unbound {
            addr: server_addr(),
            unread: 0,
        },
        NetEvent::Bound {
            addr: server_addr(),
        },
    ];
    assert!(net.events().windows(2).any(|pair| pair == rebind));
}
