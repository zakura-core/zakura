//! M5-B: `Net`, the `SimNet` action source, under the step loop. Real QUIC
//! runs on it, time stops at a datagram's deadline, and its clock map holds
//! wherever a run starts.
#![cfg(zakura_sim)]

mod support;

use std::{
    cell::RefCell,
    future::Future,
    net::{Ipv4Addr, SocketAddr},
    pin::Pin,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use zakura_quic::{
    sim::{LinkModel, SeededInputs, SimNet, SimSocket},
    Acceptor, Admit, Conn, IncomingInfo, NodeAddr, QuicBindConfig, QuicConfig, QuicEndpoint,
    Transmit, UdpIo,
};
use zakura_scheduler::{SchedulerConfig, SimInstant};
use zakura_sim::{
    check,
    harness::{LinkEntry, LinkSpec, NetScenario},
    net::Net,
    observe::DatagramOutcome,
    ActionSource, Boundary, Identity, IdleStatus, MemorySink, NoChecks, Observation,
    ObservationLog, Outcome, Plan, PlanInput, RunConfig, RunReport, Sim,
};

use support::{advances, picks};

/// The ALPN both endpoints speak.
const ALPN: &[u8] = b"p2p-v2/3";

/// What the client sends and the server echoes.
const PAYLOAD: &[u8] = b"m5 net payload!";

/// `10.0.0.n:4000`.
fn addr(n: u8) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::new(10, 0, 0, n), 4000))
}

/// A run config with the default scheduler and room for a QUIC handshake.
fn run_config() -> RunConfig {
    RunConfig {
        scheduler: SchedulerConfig::default(),
        budget: 200_000,
        ..RunConfig::default()
    }
}

/// A network whose links all follow `link`.
fn scenario(link: LinkSpec) -> NetScenario {
    NetScenario {
        seed: [0x5a; 32],
        default_link: link,
        ..NetScenario::default()
    }
}

/// A link with a fixed 0 to `max_ms` window and no loss.
fn link(max_ms: u64, reorder: bool) -> LinkSpec {
    LinkSpec {
        min_delay_ms: 0,
        max_delay_ms: max_ms,
        reorder,
        loss_budget: 0,
    }
}

/// Sends one `len`-byte datagram from `socket` to `dst`.
fn send(socket: &SimSocket, dst: SocketAddr, len: usize) {
    let transmit = Transmit {
        destination: dst,
        ecn: None,
        size: len,
        segment_size: None,
        src_ip: None,
    };
    socket
        .try_send(&transmit, &vec![0; len])
        .expect("a simulated send never fails");
}

/// `t` as `SimInstant`.
fn at(t: Duration) -> SimInstant {
    SimInstant::from_start(t)
}

/// The identities `net` enables now.
fn enabled(net: &Net) -> Vec<String> {
    let mut out = Vec::new();
    net.enabled(SimInstant::ZERO, &mut out);
    let mut shown: Vec<String> = out.iter().map(|(id, _)| id.to_string()).collect();
    shown.sort();
    shown
}

/// Each datagram observation in `report`'s log, with the virtual time of
/// the step that logged it.
fn datagrams(report: &RunReport) -> Vec<(u64, Observation)> {
    let mut now = 0;
    let mut out = Vec::new();
    for entry in report.log.entries() {
        match &entry.obs {
            Observation::TimeAdvanced { to_ns, .. } => now = *to_ns,
            obs @ Observation::Datagram { .. } => out.push((now, obs.clone())),
            _ => {}
        }
    }
    out
}

// The M3 echo scenario, from zakura-quic's `tests/sim_replay.rs`.

/// Retries every unvalidated attempt, counting the Retries, and echoes one
/// stream per connection.
struct EchoAcceptor {
    retries: Arc<AtomicUsize>,
}

impl Acceptor for EchoAcceptor {
    /// A Retry first, then accept once the address is validated.
    fn admit(&self, incoming: &IncomingInfo) -> Admit {
        if incoming.validated {
            Admit::Accept
        } else {
            self.retries.fetch_add(1, Ordering::Relaxed);
            Admit::Retry
        }
    }

    /// The one test ALPN.
    fn alpns(&self) -> Vec<Vec<u8>> {
        vec![ALPN.to_vec()]
    }

    /// Echoes the first stream, then waits for the client to close.
    fn handle(&self, conn: Conn) -> Pin<Box<dyn Future<Output = ()> + Send>> {
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

/// The transport config for both nodes, as M3's.
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

/// Binds a server at `10.0.0.1` and a client at `10.0.0.2` on `net`, has the
/// client dial through a Retry and echo [`PAYLOAD`], then shuts both down.
/// Returns how it ended.
async fn handshake_and_echo(seed: [u8; 32], net: SimNet, retries: Arc<AtomicUsize>) -> String {
    let server = QuicEndpoint::bind_with(
        SeededInputs::node_key(seed, 0),
        &bind_config(addr(1)),
        &quic_config(),
        net.clone(),
        SeededInputs::new(seed, 0).endpoint_inputs(),
    )
    .expect("the server binds");
    server
        .serve(EchoAcceptor { retries })
        .expect("the server serves");
    let client = QuicEndpoint::bind_with(
        SeededInputs::node_key(seed, 1),
        &bind_config(addr(2)),
        &quic_config(),
        net,
        SeededInputs::new(seed, 1).endpoint_inputs(),
    )
    .expect("the client binds");

    let target = NodeAddr::with_addrs(server.local_id(), [addr(1)]);
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

/// Runs the echo on a [`Sim`] over `Net` with sim.md's default links, under
/// `plan`. Returns the report, how the echo ended, and how many Retries the
/// server sent.
fn echo(plan: PlanInput) -> (RunReport, String, usize) {
    let scenario = scenario(LinkSpec::default());
    let mut sim = Sim::new(run_config(), plan);
    let net = scenario.net();
    sim.add_source(Box::new(net.clone()));
    let outcome = Rc::new(RefCell::new(None));
    let retries = Arc::new(AtomicUsize::new(0));
    {
        let (outcome, retries, seed) = (Rc::clone(&outcome), Arc::clone(&retries), scenario.seed);
        sim.scheduler().spawn_root("scenario", async move {
            let ended = handshake_and_echo(seed, net.sim(), retries).await;
            *outcome.borrow_mut() = Some(ended);
        });
    }
    let report = sim.run(&mut NoChecks, &mut MemorySink::default());
    let outcome = outcome.borrow_mut().take().unwrap_or_default();
    (report, outcome, retries.load(Ordering::Relaxed))
}

/// M3's Retry-then-echo completes on the step loop with `Net` choosing every
/// delivery, every datagram arrives within its window, and two runs of one
/// plan in one process observe the same things.
#[test]
fn echo_runs_under_sim() {
    for plan in [
        Plan::from_fuzz(&[]),
        Plan::from_fuzz(&[7, 200, 3, 91, 18, 255, 64, 5]),
    ] {
        let (first, outcome, retries) = echo(plan.clone());
        assert_eq!(outcome, "echoed", "{:?}", first.end);
        assert_eq!(
            retries, 1,
            "the server answered the first Initial with a Retry"
        );
        assert_eq!(
            first.end.outcome,
            Outcome::Idle {
                status: IdleStatus::Quiescent
            },
            "{:?}",
            first.end
        );

        let mut sent = std::collections::BTreeMap::new();
        let mut delivered = 0;
        for (now, obs) in datagrams(&first) {
            let Observation::Datagram {
                id,
                link,
                len,
                outcome,
                sent_at,
            } = obs
            else {
                unreachable!("`datagrams` keeps only datagrams");
            };
            match outcome {
                DatagramOutcome::Sent => {
                    assert_eq!(sent_at, now, "a send is stamped with its step's time");
                    sent.insert((link, id), (len, sent_at));
                }
                DatagramOutcome::Delivered => {
                    let window = sent_at..=sent_at + 100_000_000;
                    assert!(window.contains(&now), "{link}:{id} arrived at {now}");
                    assert_eq!(sent.get(&(link, id)), Some(&(len, sent_at)));
                    delivered += 1;
                }
                other => panic!("{link}:{id} was {other:?} on a lossless network"),
            }
        }
        assert!(
            delivered >= 6,
            "the handshake and echo took {delivered} datagrams"
        );

        let (second, _, _) = echo(plan);
        assert_eq!(
            second.log.entries(),
            first.log.entries(),
            "the same observations"
        );
        assert_eq!(picks(&second), picks(&first), "the same picks");
    }
}

/// Runs one datagram from `10.0.0.1` to `10.0.0.2` on a 0 to 100 ms link.
/// The `Net` is made after `before` of virtual time, and the datagram is
/// sent `after` that. Returns every deadline the source reported and the
/// send's stamp.
fn deadlines_for_a_send(before: Duration, after: Duration) -> (Vec<SimInstant>, u64) {
    let mut sim = Sim::new(run_config(), Plan::from_fuzz(&[]));
    if !before.is_zero() {
        sim.scheduler().advance_to(at(before));
    }
    let net = scenario(link(100, true)).net();
    sim.add_source(Box::new(net.clone()));
    let sim_net = net.sim();
    let (a, b) = (
        sim_net.bind(addr(1)).unwrap(),
        sim_net.bind(addr(2)).unwrap(),
    );
    sim.scheduler().spawn_root("t", async move {
        zakura_rt::time::sleep(after).await;
        send(&a, addr(2), 10);
    });
    let deadlines = RefCell::new(Vec::new());
    let mut checker = check::after_step(|cx| {
        let mut out = Vec::new();
        net.boundaries(cx.now, &mut out);
        deadlines
            .borrow_mut()
            .extend(out.into_iter().filter_map(|b| match b {
                Boundary::Deadline(t) => Some(t),
                Boundary::Opens(_) => None,
            }));
        None
    });
    let report = sim.run(&mut checker, &mut MemorySink::default());
    drop((checker, b));
    let sent_at = datagrams(&report)
        .into_iter()
        .find_map(|(_, obs)| match obs {
            Observation::Datagram {
                outcome: DatagramOutcome::Sent,
                sent_at,
                ..
            } => Some(sent_at),
            _ => None,
        })
        .expect("the datagram was sent");
    (deadlines.into_inner(), sent_at)
}

/// The clock map holds after virtual time has moved and in a second run:
/// a datagram sent at `t` on a 100 ms link has its deadline at exactly
/// `t + 100 ms`, whether the run made its `Net` at 0 or 3 s in.
#[test]
fn clock_map_survives_advanced_time() {
    for (before, after) in [
        (Duration::ZERO, Duration::from_secs(3)),
        (Duration::from_secs(3), Duration::from_millis(500)),
    ] {
        let sent = before + after;
        let (deadlines, sent_at) = deadlines_for_a_send(before, after);
        assert_eq!(u128::from(sent_at), sent.as_nanos(), "the send's stamp");
        assert_eq!(
            deadlines.first(),
            Some(&at(sent + Duration::from_millis(100))),
            "made at {before:?}, sent at {sent:?}: {deadlines:?}"
        );
    }
}

/// With a datagram in flight and a timer 1 s past its deadline, a plan that
/// prefers advancing time stops the clock at the deadline, and the datagram
/// is delivered there.
///
/// The link's 50 ms minimum keeps the delivery closed while the sending task
/// is still ready, so every real choice is between the delivery and an
/// advance, and the plan's 0xff bytes always take the advance.
#[test]
fn time_stops_at_a_datagram_deadline() {
    let mut sim = Sim::new(run_config(), Plan::from_fuzz(&[0xff; 64]));
    let window = LinkSpec {
        min_delay_ms: 50,
        ..link(100, true)
    };
    let net = scenario(window).net();
    sim.add_source(Box::new(net.clone()));
    let sim_net = net.sim();
    let (a, b) = (
        sim_net.bind(addr(1)).unwrap(),
        sim_net.bind(addr(2)).unwrap(),
    );
    sim.scheduler().spawn_root("t", async move {
        send(&a, addr(2), 10);
        zakura_rt::time::sleep(Duration::from_millis(1_100)).await;
    });
    let report = sim.run(&mut NoChecks, &mut MemorySink::default());
    drop(b);

    let delivered: Vec<u64> = datagrams(&report)
        .into_iter()
        .filter_map(|(now, obs)| {
            matches!(
                obs,
                Observation::Datagram {
                    outcome: DatagramOutcome::Delivered,
                    ..
                }
            )
            .then_some(now)
        })
        .collect();
    assert_eq!(delivered, [100_000_000], "delivered at the deadline");
    assert_eq!(
        advances(&report),
        [50_000_000, 100_000_000, 1_100_000_000],
        "time stopped at the window and the deadline before the timer"
    );
    let choices: Vec<&str> = report
        .picks
        .iter()
        .filter(|pick| pick.options > 1)
        .map(|pick| pick.identity.as_str())
        .collect();
    assert_eq!(
        choices,
        ["advance-time:100000000"],
        "the plan preferred time"
    );
}

/// On a link without reordering, the source offers only that link's oldest
/// datagram, and the next once that one goes. A reordering link offers all
/// of its own.
#[test]
fn fifo_link_offers_only_the_oldest() {
    let sim = Sim::new(run_config(), Plan::from_fuzz(&[]));
    let reordering = |src, dst| LinkEntry {
        src: addr(src),
        dst: addr(dst),
        model: link(100, true),
    };
    let mut net = NetScenario {
        links: vec![reordering(3, 4)],
        ..scenario(link(100, false))
    }
    .net();
    let sim_net = net.sim();
    let sockets: Vec<SimSocket> = (1..=4).map(|n| sim_net.bind(addr(n)).unwrap()).collect();
    for _ in 0..3 {
        send(&sockets[0], addr(2), 10);
    }
    for _ in 0..2 {
        send(&sockets[1], addr(1), 10);
        send(&sockets[2], addr(4), 10);
    }
    assert_eq!(
        enabled(&net),
        [
            "deliver:10.0.0.1:4000->10.0.0.2:4000:0",
            "deliver:10.0.0.2:4000->10.0.0.1:4000:0",
            "deliver:10.0.0.3:4000->10.0.0.4:4000:0",
            "deliver:10.0.0.3:4000->10.0.0.4:4000:1",
        ]
    );

    let mut offers = Vec::new();
    net.enabled(SimInstant::ZERO, &mut offers);
    let (_, local) = offers
        .iter()
        .find(|(id, _)| *id == Identity::deliver("10.0.0.1:4000->10.0.0.2:4000", 0))
        .expect("the oldest is offered");
    net.execute(*local, &mut ObservationLog::default());
    assert!(enabled(&net).contains(&"deliver:10.0.0.1:4000->10.0.0.2:4000:1".to_owned()));
    drop(sockets);
    drop(sim);
}

/// `enabled` and `boundaries` answer for the `now` they're given, not the
/// run's clock: with the run at 0 and a datagram's window at 50 to 100 ms,
/// asking at 50 ms offers it and asking at 60 ms shows only the deadline.
#[test]
fn queries_answer_for_the_supplied_instant() {
    let _sim = Sim::new(run_config(), Plan::from_fuzz(&[]));
    let window = LinkSpec {
        min_delay_ms: 50,
        ..link(100, true)
    };
    let net = scenario(window).net();
    let sim_net = net.sim();
    let (a, _b) = (
        sim_net.bind(addr(1)).unwrap(),
        sim_net.bind(addr(2)).unwrap(),
    );
    send(&a, addr(2), 10);
    let ms = |n| at(Duration::from_millis(n));
    let offers = |now| {
        let mut out = Vec::new();
        net.enabled(now, &mut out);
        out.into_iter()
            .map(|(id, _)| id.to_string())
            .collect::<Vec<_>>()
    };
    let bounds = |now| {
        let mut out = Vec::new();
        net.boundaries(now, &mut out);
        out
    };
    assert_eq!(offers(ms(0)), Vec::<String>::new());
    assert_eq!(offers(ms(50)), ["deliver:10.0.0.1:4000->10.0.0.2:4000:0"]);
    assert_eq!(
        bounds(ms(0)),
        [Boundary::Opens(ms(50)), Boundary::Deadline(ms(100))]
    );
    assert_eq!(bounds(ms(60)), [Boundary::Deadline(ms(100))]);
    assert_eq!(
        bounds(ms(150)),
        [Boundary::Deadline(ms(150))],
        "a due deadline is now"
    );
}

/// A `Net` made 3 s into a run answers for instants before its origin too:
/// a datagram sent at 3 s isn't offered at 2 s, and its window still opens
/// at 3 s.
#[test]
fn queries_before_the_origin_answer_for_that_instant() {
    let mut sim = Sim::new(run_config(), Plan::from_fuzz(&[]));
    sim.scheduler().advance_to(at(Duration::from_secs(3)));
    let net = scenario(link(100, true)).net();
    let sim_net = net.sim();
    let (a, _b) = (
        sim_net.bind(addr(1)).unwrap(),
        sim_net.bind(addr(2)).unwrap(),
    );
    send(&a, addr(2), 10);
    let ms = |n| at(Duration::from_millis(n));
    let offered = |now| {
        let mut out = Vec::new();
        net.enabled(now, &mut out);
        out.len()
    };
    let mut bounds = Vec::new();
    net.boundaries(ms(2_000), &mut bounds);
    assert_eq!(
        offered(ms(2_000)),
        0,
        "nothing is deliverable before the send"
    );
    assert_eq!(
        bounds,
        [Boundary::Opens(ms(3_000)), Boundary::Deadline(ms(3_100))]
    );
    assert_eq!(offered(ms(3_000)), 1, "deliverable once sent");
}

/// With `loss_after_admission`, a link offers no drop until the harness
/// says a session was admitted on it, and then both of its directions do.
#[test]
fn loss_waits_for_admission() {
    let lossy = LinkSpec {
        loss_budget: 1,
        ..link(100, true)
    };
    for gated in [false, true] {
        let sim = Sim::new(run_config(), Plan::from_fuzz(&[]));
        let net = NetScenario {
            loss_after_admission: gated,
            ..scenario(lossy)
        }
        .net();
        let sim_net = net.sim();
        let (a, b) = (
            sim_net.bind(addr(1)).unwrap(),
            sim_net.bind(addr(2)).unwrap(),
        );
        send(&a, addr(2), 10);
        send(&b, addr(1), 10);
        let drops = |net: &Net| -> usize {
            enabled(net)
                .iter()
                .filter(|id| id.starts_with("drop:"))
                .count()
        };
        assert_eq!(drops(&net), if gated { 0 } else { 2 }, "gated: {gated}");
        net.admitted(addr(2), addr(1));
        assert_eq!(drops(&net), 2, "gated: {gated}, after admission");
        drop((a, b));
        drop(sim);
    }
}

/// `LinkSpec`'s default is `LinkModel`'s, sim.md's header-sync link.
#[test]
fn link_spec_defaults_to_the_sim_md_link() {
    assert_eq!(LinkModel::from(LinkSpec::default()), LinkModel::default());
}
