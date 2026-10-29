//! M5-F: connection lifecycle on `header-sync/wire`, and the saved Plans.
//! A replacement while storage is pending follows M4's ordering over real
//! connections, a disconnect with pages in flight ends cleanly, and the
//! committed Plans replay to their pinned end records: the spec's
//! completion evidence. A history a test needs that never happened fails
//! it: a missing observation is a harness failure, never evidence.
#![cfg(zakura_sim)]

mod support;

use std::{cell::Cell, fs, path::PathBuf, time::Duration};

use sha2::{Digest, Sha256};
use support::{
    inputs,
    steer::{before, steer, Steer},
};
use zakura_sim::{
    domain::session::HONEST_NEVER_PUNISHED,
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            Chain, Policy,
        },
        header_sync::{HeaderWire, WirePorts, WireScenario},
        run_prepared,
        wire_plans::SavedPlan,
        HarnessError, Subsystem,
    },
    model::{is_fault_cause, Label},
    observe::{
        DatagramOutcome, Entry, ReactorCall, ReactorFact, StorageFact, StorageRequest,
        StorageResult, TransportFact,
    },
    peer::{honest::HonestSpec, peer_addr, PeerAction, PeerSpec, NODE_ADDR},
    Checker, MemorySink, NoChecks, Observation, Plan, PlanInput, PropertyOutcome, RunReport,
    StepCtx,
};

/// The node's chain at the start of most runs.
const PRELOAD: u32 = 50;

/// The ID of header sync's generated `conformant-never-closes`.
const NEVER_CLOSES: &str = "header-sync/s5v9/conformant-never-closes";

/// The reactor's request deadline, which every hold releases before.
const REQUEST_DEADLINE: Duration = Duration::from_secs(30);

/// Runs `scenario` under `plan` with `extra`, and returns the report, even
/// of a run that ended in a harness failure, with its records and the
/// ports.
fn run_with(
    scenario: &WireScenario,
    plan: PlanInput,
    extra: &mut dyn Checker,
) -> (RunReport, Vec<String>, WirePorts) {
    let ports = HeaderWire::prepare(scenario);
    let mut sink = MemorySink::default();
    let ran = run_prepared::<HeaderWire>(scenario, ports.clone(), plan, &mut sink, Some(extra));
    let report = match ran {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{error}"),
    };
    (report, sink.lines(), ports)
}

/// An honest peer holding `chain` headers, as `spec`, with `script`.
fn honest(spec: PeerSpec, chain: u32, script: Vec<PeerAction>) -> PeerSpec {
    PeerSpec {
        honest: Some(HonestSpec {
            chain,
            subscribe: None,
        }),
        script,
        ..spec
    }
}

/// The plans the lifecycle tests run: all defaults, then two byte-chosen.
fn plans() -> Vec<PlanInput> {
    let mut plans = vec![Plan::from_fuzz(&[])];
    plans.extend(inputs(2, 16_384));
    plans
}

/// The virtual time of each log entry of `report`.
fn times(report: &RunReport) -> Vec<Duration> {
    let mut at = Duration::ZERO;
    report
        .log
        .entries()
        .iter()
        .map(|entry| {
            if let Observation::TimeAdvanced { to_ns, .. } = entry.obs {
                at = Duration::from_nanos(to_ns);
            }
            at
        })
        .collect()
}

/// The position of the first entry of `entries` that `pred` accepts.
fn find(entries: &[Entry], pred: impl Fn(&Observation) -> bool) -> Option<usize> {
    entries.iter().position(|entry| pred(&entry.obs))
}

/// Where the first log entry of a step at or after `step` sits.
fn at_step(entries: &[Entry], step: u64) -> usize {
    entries.partition_point(|entry| entry.step < step)
}

/// The step the plan picked `identity` at.
fn picked(report: &RunReport, identity: &str) -> u64 {
    report
        .picks
        .iter()
        .find(|pick| pick.identity == identity)
        .unwrap_or_else(|| panic!("the plan never picked {identity}: {:?}", report.end))
        .step
}

/// A transport fact on `connection`, as `pred` accepts it, and where.
fn transport(
    entries: &[Entry],
    connection: u64,
    pred: impl Fn(&TransportFact) -> bool,
) -> Option<usize> {
    find(entries, |obs| {
        matches!(obs, Observation::Transport { connection: Some(c), event, .. }
            if *c == connection && pred(event))
    })
}

/// A header storage operation the node submitted.
#[derive(Clone, Debug)]
struct Op {
    id: u64,
    kind: OpKind,
    /// Its owner's session.
    session: u64,
    /// Where its submission sits.
    submitted: usize,
}

/// Every prepare and apply the node submitted, in order.
fn ops(entries: &[Entry]) -> Vec<Op> {
    entries
        .iter()
        .enumerate()
        .filter_map(|(at, entry)| match &entry.obs {
            Observation::Storage {
                instance: 0,
                fact: StorageFact::Submitted { id, request, .. },
                ..
            } => {
                let (kind, owner) = match request {
                    StorageRequest::PrepareHeaderTarget { owner, .. } => (OpKind::Prepare, owner),
                    StorageRequest::ApplyHeaderTarget { owner, .. } => (OpKind::Apply, owner),
                    _ => return None,
                };
                Some(Op {
                    id: *id,
                    kind,
                    session: owner.session_id,
                    submitted: at,
                })
            }
            _ => None,
        })
        .collect()
}

/// Operation `id`'s completion: where it sits, its outcome, result and the
/// header generation after it.
fn completion(entries: &[Entry], id: u64) -> Option<(usize, &str, &StorageResult, u64)> {
    entries
        .iter()
        .enumerate()
        .find_map(|(at, entry)| match &entry.obs {
            Observation::Storage {
                instance: 0,
                fact:
                    StorageFact::Completed {
                        id: done,
                        outcome,
                        result,
                        generation,
                        ..
                    },
                ..
            } if *done == id => Some((at, outcome.as_str(), result, *generation)),
            _ => None,
        })
}

/// The header generation just before log position `at`: that of the last
/// completion before it.
fn generation_before(entries: &[Entry], at: usize) -> Option<u64> {
    entries[..at]
        .iter()
        .rev()
        .find_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0,
                fact: StorageFact::Completed { generation, .. },
                ..
            } => Some(*generation),
            _ => None,
        })
}

/// The reactor's port call paired with operation `op`: its number, the
/// owner session it dispatched for, and where its `CallEnded` and
/// `Handled` sit.
fn call_of(entries: &[Entry], op: u64) -> Option<(u64, u64, usize, Option<usize>)> {
    let (ended, call) = entries
        .iter()
        .enumerate()
        .find_map(|(at, entry)| match &entry.obs {
            Observation::Reactor {
                event:
                    ReactorFact::CallEnded {
                        call, op: Some(o), ..
                    },
                ..
            } if *o == op => Some((at, *call)),
            _ => None,
        })?;
    let session = entries.iter().find_map(|entry| match &entry.obs {
        Observation::Reactor {
            event:
                ReactorFact::Dispatched {
                    call: c,
                    port: ReactorCall::Prepare { owner, .. } | ReactorCall::Apply { owner, .. },
                },
            ..
        } if *c == call => Some(owner.session_id),
        _ => None,
    })?;
    let handled = find(
        entries,
        |obs| matches!(obs, Observation::Reactor { event: ReactorFact::Handled { call: c, .. }, .. } if *c == call),
    );
    Some((call, session, ended, handled))
}

/// M4's replacement while a prepare is pending (`header_node_late.rs`,
/// `prepare_completed_after_replacement_is_inert`), over real connections,
/// with every session ID read back from the node's `admitted` events.
///
/// 1. Session 1, `peer-0`'s first connection, has its prepare held: the
///    node syncs 70 headers from it and prepares them.
/// 2. `peer-0` closes that connection, and the node tears it down. A
///    newcomer for a live identity is closed (`handler.rs:1678`), so the old
///    connection goes first: its `torn-down` comes before the new one's
///    `registered`.
/// 3. `peer-0` dials again from a fresh address, after the production 10 s
///    handshake timeout. Session 2 is admitted, then submits its own
///    prepare.
/// 4. Session 2's prepare and apply are held.
/// 5. Only then does session 1's prepare complete, with the engine's `Ok`.
/// 6. Once the reactor has handled that completion, the old callback has
///    retired none of session 2's work and dispatched no apply: nothing
///    between its `CallEnded` and `Handled` does either, the call's owner is
///    session 1's, and the header generation is the same at both. No apply
///    ever carries session 1.
/// 7. Session 2's holds release, its apply commits, the reactor handles
///    that apply, and session 2's request ends after the commit, admitted
///    inside the apply's bracket or made obsolete by it. The node reaches
///    the tip.
///
/// Every hold releases before the reactor's 30 s request deadline. A step
/// of the history that isn't reached fails the test.
#[test]
fn replacement_while_storage_is_pending() {
    let handled = Trigger::Handled(Select::nth(OpKind::Prepare, 1));
    let hold = |select, until, deadline_ms| Hold {
        select,
        until,
        then: vec![Allow::Engine],
        deadline_ms: Some(deadline_ms),
    };
    let scenario = WireScenario {
        node: Chain::main(PRELOAD),
        peers: vec![honest(
            PeerSpec::dialing(),
            120,
            vec![PeerAction::Close, PeerAction::Redial],
        )],
        policy: Policy {
            holds: vec![
                hold(
                    Select::nth(OpKind::Prepare, 1),
                    Trigger::Submitted(Select::nth(OpKind::Prepare, 2)),
                    20_000,
                ),
                hold(Select::nth(OpKind::Prepare, 2), handled.clone(), 25_000),
                hold(Select::nth(OpKind::Apply, 1), handled, 5_000),
            ],
            ..Policy::converging()
        },
        ..WireScenario::default()
    };
    for plan in plans() {
        let (report, _, ports) = run_with(&scenario, plan, &mut NoChecks);
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        assert_eq!(report.end.violation, None, "{:?}", report.end);
        let entries = report.log.entries();
        let times = times(&report);
        let sessions: Vec<(u64, String, u64, usize)> = entries
            .iter()
            .enumerate()
            .filter_map(|(at, entry)| match &entry.obs {
                Observation::Session {
                    connection,
                    peer,
                    session_id,
                    ..
                } => Some((*connection, peer.clone(), *session_id, at)),
                _ => None,
            })
            .collect();
        assert_eq!(
            sessions.len(),
            2,
            "one session per connection: {sessions:?}"
        );
        let [(old_conn, _, old, _), (new_conn, _, new, new_admitted)] =
            [sessions[0].clone(), sessions[1].clone()];
        assert_ne!(old_conn, new_conn);
        assert_ne!(old, new, "the replacement has its own session");
        let remote_of = |connection: u64| {
            entries.iter().find_map(|entry| match &entry.obs {
                Observation::Transport {
                    connection: Some(c),
                    remote,
                    event: TransportFact::Registered,
                    ..
                } if *c == connection => Some(remote.clone()),
                _ => None,
            })
        };
        assert_eq!(remote_of(old_conn), Some(peer_addr(0, 0).to_string()));
        assert_eq!(remote_of(new_conn), Some(peer_addr(0, 1).to_string()));

        // 1. Session 1's prepare, submitted before the close.
        let ops = ops(entries);
        let prepares: Vec<&Op> = ops.iter().filter(|op| op.kind == OpKind::Prepare).collect();
        let applies: Vec<&Op> = ops.iter().filter(|op| op.kind == OpKind::Apply).collect();
        assert_eq!(prepares.len(), 2, "one prepare per session: {prepares:?}");
        let (p1, p2) = (prepares[0], prepares[1]);
        assert_eq!((p1.session, p2.session), (old, new));
        let closed = at_step(entries, picked(&report, "peer:peer-0:close"));
        assert!(p1.submitted < closed, "session 1 prepared before the close");

        // 2. The old connection closed and tore down before the new one
        // registered.
        let decided = transport(entries, old_conn, |fact| {
            matches!(fact, TransportFact::CloseDecided { .. })
        })
        .expect("the node decided the old connection's close");
        let torn = transport(entries, old_conn, |fact| {
            matches!(fact, TransportFact::TornDown)
        })
        .expect("the old connection tore down");
        let registered = transport(entries, new_conn, |fact| {
            matches!(fact, TransportFact::Registered)
        })
        .expect("the redial registered");
        let peer_closed = find(entries, |obs| {
            matches!(obs, Observation::PeerClosed { connection, .. } if *connection == old_conn)
        })
        .expect("the peer closed the old connection");
        assert!(
            closed <= peer_closed && peer_closed < decided && decided < torn && torn < registered
        );

        // 3. Session 2 was admitted, then submitted its own prepare.
        assert!(new_admitted < p2.submitted);

        // 4 and 5. Session 1's prepare completed only once session 2's was
        // submitted, with the engine's Ok, while session 2's work was held.
        let (p1_done, outcome, result, _) =
            completion(entries, p1.id).expect("session 1's prepare completed");
        assert!(p2.submitted < p1_done);
        assert_eq!((outcome, result), ("engine", &StorageResult::Prepared));
        let (p2_done, ..) = completion(entries, p2.id).expect("session 2's prepare completed");

        // 6. The old completion was handled without touching session 2.
        let (_, owner, ended, handled) = call_of(entries, p1.id).expect("the old call is paired");
        let handled = handled.expect("the reactor handled the old completion");
        assert_eq!(owner, old, "the old call is session 1's");
        assert!(handled < p2_done, "session 2's prepare waited for it");
        for entry in &entries[ended + 1..handled] {
            if let Observation::Reactor { event, .. } = &entry.obs {
                assert!(
                    !matches!(event, ReactorFact::Retired { request, .. } if request.session_id == new)
                        && !matches!(
                            event,
                            ReactorFact::Dispatched {
                                port: ReactorCall::Apply { .. },
                                ..
                            }
                        ),
                    "the old completion retired or applied: {event:?}"
                );
            }
        }
        assert_eq!(
            generation_before(entries, ended),
            generation_before(entries, handled),
            "no commit decided the old result"
        );
        assert!(
            applies.iter().all(|op| op.session == new),
            "no apply carries session 1: {applies:?}"
        );

        // 7. Session 2 committed on its own.
        assert_eq!(applies.len(), 1, "{applies:?}");
        let (a_done, outcome, result, generation) =
            completion(entries, applies[0].id).expect("session 2's apply completed");
        assert!(handled < applies[0].submitted, "the apply waited for it");
        assert_eq!((outcome, result), ("engine", &StorageResult::Applied));
        assert_eq!(
            Some(generation),
            generation_before(entries, a_done).map(|before| before + 1),
            "session 2's apply committed"
        );
        let (_, owner, apply_ended, apply_handled) =
            call_of(entries, applies[0].id).expect("session 2's apply call is paired");
        assert_eq!(owner, new);
        let apply_handled = apply_handled.expect("the reactor handled session 2's apply");
        // Session 2's request ended after its commit: admitted inside its
        // apply's bracket, or made obsolete by a commit past its owner's
        // generation.
        let retired: Vec<(usize, &str)> = entries
            .iter()
            .enumerate()
            .filter_map(|(at, entry)| match &entry.obs {
                Observation::Reactor {
                    event:
                        ReactorFact::Retired {
                            request, reason, ..
                        },
                    ..
                } if request.session_id == new => Some((at, reason.as_str())),
                _ => None,
            })
            .collect();
        assert!(!retired.is_empty(), "session 2's request ended");
        for (at, reason) in &retired {
            assert!(
                *at > a_done,
                "session 2 was retired before its commit: {reason}"
            );
            let admitted = *reason == "target_admitted" && apply_ended < *at && *at < apply_handled;
            assert!(
                admitted || *reason == "snapshot_obsolete",
                "session 2 was retired as {reason}"
            );
        }
        assert_eq!(ports.selected_tip(), Chain::main(120).tip(&ports.fixture));

        // Every hold released before the request deadline.
        for (op, done) in [(p1, p1_done), (p2, p2_done), (applies[0], a_done)] {
            let held = times[done] - times[op.submitted];
            assert!(held < REQUEST_DEADLINE, "{op:?} held {held:?}");
        }
        assert_eq!(ports.unplaced_records(), 0);
    }
}

/// The smallest datagram that counts as carrying page bytes: a full QUIC
/// packet, unlike an acknowledgement.
const DATA_DATAGRAM: u64 = 1_000;

/// One datagram record on a link, with where it sits.
#[derive(Clone, Copy, Debug)]
struct Sent {
    at: usize,
    id: u64,
    len: u64,
    /// Whether it's still in the network at the end of the entries read.
    in_flight: bool,
}

/// Every datagram sent on `link` among `entries`, in order.
fn sent_on(entries: &[Entry], link: &str) -> Vec<Sent> {
    let mut sent: Vec<Sent> = Vec::new();
    for (at, entry) in entries.iter().enumerate() {
        if let Observation::Datagram {
            id,
            link: l,
            len,
            outcome,
            ..
        } = &entry.obs
        {
            if l != link {
                continue;
            }
            if *outcome == DatagramOutcome::Sent {
                sent.push(Sent {
                    at,
                    id: *id,
                    len: *len,
                    in_flight: true,
                });
            } else if let Some(datagram) = sent.iter_mut().find(|d| d.id == *id) {
                datagram.in_flight = false;
            }
        }
    }
    sent
}

/// `peer-0` serves the node from 1,200 headers, and a focused Plan has it
/// close its connection with page bytes still in the network:
///
/// 1. From the peer's first page on, its datagrams to the node wait.
/// 2. Once three full-size ones are waiting, the Plan picks `disconnect`.
///    Acknowledgements and the close are small, and the peer's MTU probes
///    go one at a time, so at least two of them carry page bytes.
/// 3. The peer's task closes the connection and logs `PeerClosed`, with
///    those datagrams still in the network, and then they may go.
///
/// The peer sends nothing after its close but the close itself, so a page
/// whose bytes it hadn't sent yet never reaches the node whole. The node
/// decides the close with a neutral cause after the peer's `PeerClosed`,
/// and tears the connection down. No task leaks at shutdown, and no page
/// the node never read whole has a `FrameJudged`: there's at least one, and
/// none of them is judged.
///
/// This fails if the close came with no page bytes in flight, if every
/// page was read anyway, or if a page the node never read is judged.
#[test]
fn disconnect_while_pages_are_in_flight() {
    let peer = peer_addr(0, 0);
    let scenario = WireScenario {
        node: Chain::main(PRELOAD),
        peers: vec![honest(
            PeerSpec::dialing(),
            1_200,
            vec![PeerAction::Disconnect],
        )],
        policy: Policy::converging(),
        ..WireScenario::default()
    };
    let to_node = format!("{peer}->{NODE_ADDR}");
    let deliver = format!("deliver:{to_node}:");
    let disconnect = "peer:peer-0:disconnect";
    // Whether the plan has disconnected, in the run the rule is judging.
    let disconnected = Cell::new(false);
    let rule = |cx: &StepCtx<'_>| {
        if cx.step == 1 {
            disconnected.set(false);
        }
        let entries = before(cx);
        let marks = Marks::of(entries, &to_node);
        let pick = cx.pick.to_string();
        // A datagram to the node that waits now.
        let held = |identity: &str| {
            let Some(id) = identity.strip_prefix(&deliver) else {
                return false;
            };
            let id: u64 = id.parse().expect("a datagram's id");
            marks.sent.iter().any(|d| d.id == id && marks.waits(d))
        };
        let other = |keep: &dyn Fn(&str) -> bool| {
            cx.enabled
                .iter()
                .map(|e| e.identity.to_string())
                .find(|identity| keep(identity))
                .map(Steer::Pick)
                .ok_or_else(|| "only held actions are enabled".to_owned())
        };
        let want = if disconnected.get() {
            if held(&pick) {
                other(&|identity| !held(identity))?
            } else {
                Steer::Fine
            }
        } else {
            let offered = cx
                .enabled
                .iter()
                .any(|e| e.identity.to_string() == disconnect);
            if offered && marks.full_waiting() >= 3 {
                Steer::Pick(disconnect.to_owned())
            } else if pick == disconnect || held(&pick) {
                other(&|identity| identity != disconnect && !held(identity))?
            } else {
                Steer::Fine
            }
        };
        if pick == disconnect && (want == Steer::Fine || want == Steer::Pick(pick.clone())) {
            disconnected.set(true);
        }
        Ok(want)
    };
    for plan in plans() {
        let mut ports = None;
        let (report, ..) = steer(plan, &rule, &mut |plan, checker| {
            let (report, lines, run_ports) = run_with(&scenario, plan, checker);
            ports = Some(run_ports);
            (report, lines)
        });
        let ports = ports.expect("a run ran");
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        assert_eq!(report.end.violation, None, "{:?}", report.end);
        assert_eq!(report.cleanup.leaked, 0, "shutdown leaked no task");
        let entries = report.log.entries();
        let marks = Marks::of(entries, &to_node);
        let connection = marks.connection.expect("the peer connected");
        let picked = at_step(entries, picked(&report, disconnect));
        let peer_closed = marks.peer_closed.expect("the peer closed");
        let decided = marks.node_closed.expect("the node decided the close");
        assert!(
            marks.paged.is_some_and(|paged| paged < picked) && picked <= peer_closed,
            "the peer paged, then disconnected, then closed"
        );
        assert!(peer_closed < decided, "the node decided the close after it");
        // Page bytes were still in the network when the peer closed.
        let waiting = Marks::of(&entries[..peer_closed], &to_node).full_waiting();
        assert!(
            waiting >= 2,
            "{waiting} full-size datagrams sent after the first page were in flight at the close"
        );
        let cause = entries[decided..]
            .iter()
            .find_map(|entry| match &entry.obs {
                Observation::Transport {
                    event: TransportFact::CloseDecided { cause },
                    ..
                } => Some(cause.clone()),
                _ => None,
            });
        let cause = cause.expect("the node decided the close");
        assert!(
            cause.as_deref().is_none_or(|cause| !is_fault_cause(cause)),
            "a neutral cause: {cause:?}"
        );
        assert!(
            transport(entries, connection, |fact| matches!(
                fact,
                TransportFact::TornDown
            ))
            .is_some(),
            "the connection tore down"
        );
        assert!(
            !entries
                .iter()
                .any(|entry| matches!(entry.obs, Observation::Misbehavior { .. })),
            "no misbehavior"
        );
        // Pages the node never read whole have no judgement.
        let pages: Vec<(u64, u64)> = entries
            .iter()
            .filter_map(|entry| match &entry.obs {
                Observation::PeerFrame {
                    quic_stream: Some(q),
                    seq,
                    row: Some(row),
                    label,
                    ..
                } if row == "headers" => {
                    assert_eq!(*label, Label::Conformant);
                    Some((*q, *seq))
                }
                _ => None,
            })
            .collect();
        let unread: Vec<&(u64, u64)> = pages
            .iter()
            .filter(|(q, seq)| {
                transport(entries, connection, |fact| {
                    matches!(fact, TransportFact::FrameRead { quic_stream, seq: s }
                        if (quic_stream, s) == (q, seq))
                })
                .is_none()
            })
            .collect();
        assert!(
            !unread.is_empty(),
            "some page was never read whole: {pages:?}"
        );
        for (q, seq) in unread {
            assert!(
                !entries.iter().any(|entry| matches!(entry.obs,
                    Observation::FrameJudged { quic_stream: Some(fq), seq: fs, .. }
                        if (fq, fs) == (*q, *seq))),
                "page {seq} was judged unread"
            );
        }
        assert_eq!(ports.unplaced_records(), 0);
    }
}

/// Where the disconnect run's milestones sit among some log entries: the
/// peer's connection, its first page, its `PeerClosed`, the node's close
/// decision, and every datagram the peer sent to the node.
struct Marks {
    connection: Option<u64>,
    paged: Option<usize>,
    peer_closed: Option<usize>,
    node_closed: Option<usize>,
    sent: Vec<Sent>,
}

impl Marks {
    /// The marks among `entries`, for datagrams on `link`.
    fn of(entries: &[Entry], link: &str) -> Self {
        Marks {
            connection: entries.iter().find_map(|entry| match entry.obs {
                Observation::Session { connection, .. } => Some(connection),
                _ => None,
            }),
            paged: find(
                entries,
                |obs| matches!(obs, Observation::PeerFrame { row: Some(row), .. } if row == "headers"),
            ),
            peer_closed: find(entries, |obs| matches!(obs, Observation::PeerClosed { .. })),
            node_closed: find(entries, |obs| {
                matches!(
                    obs,
                    Observation::Transport {
                        event: TransportFact::CloseDecided { .. },
                        ..
                    }
                )
            }),
            sent: sent_on(entries, link),
        }
    }

    /// Whether datagram `d` waits: it's in flight, sent after the first
    /// page, and the peer hasn't closed yet.
    fn waits(&self, d: &Sent) -> bool {
        self.peer_closed.is_none() && d.in_flight && self.paged.is_some_and(|paged| d.at > paged)
    }

    /// How many full-size datagrams sent after the first page and before
    /// the peer's close are in flight at the end of the entries read.
    fn full_waiting(&self) -> usize {
        self.sent
            .iter()
            .filter(|d| {
                d.in_flight
                    && d.len >= DATA_DATAGRAM
                    && self.paged.is_some_and(|paged| d.at > paged)
                    && self.peer_closed.is_none_or(|closed| d.at < closed)
            })
            .count()
    }
}

/// The committed fixture directory.
fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Saved Plan `plan`'s committed input and pinned end record.
fn committed(plan: SavedPlan) -> (Vec<u8>, String) {
    let base = fixtures().join(format!("header-wire-{}", plan.name()));
    let input = fs::read(base.with_extension("bin"))
        .unwrap_or_else(|error| panic!("{}: {error}", plan.name()));
    let end = fs::read_to_string(base.with_extension("end.json"))
        .unwrap_or_else(|error| panic!("{}: {error}", plan.name()));
    (input, end.trim_end().to_owned())
}

/// Runs a committed input as `zakura-sim record --input` does: its prefix
/// picks the scenario, and the rest is the Plan. Returns the report with
/// its end record's line.
fn replay(input: &[u8]) -> (RunReport, String, WirePorts) {
    let (prefix, plan) = input.split_at(HeaderWire::SCENARIO_BYTES);
    let scenario = HeaderWire::draw(prefix);
    let (report, lines, ports) = run_with(&scenario, Plan::from_fuzz(plan), &mut NoChecks);
    let end = lines.last().expect("a run writes its end record").clone();
    (report, end, ports)
}

/// The generated property that judges a frame labelled `label` on `row`.
fn judging(label: Label, row: &str) -> String {
    match label {
        Label::Conformant => NEVER_CLOSES.to_owned(),
        Label::UndeclaredType => "header-sync/s5v9/undeclared-type-closes".to_owned(),
        label => format!("header-sync/s5v9/{row}/{}-closes", label.name()),
    }
}

/// Each saved Plan, committed under `tests/fixtures/header-wire-*.bin`
/// with its sidecar, replays to its pinned end record,
/// `header-wire-*.end.json`. Its prefix picks the saved scenario. The
/// honest sync reaches the 120-header tip with `conformant-never-closes`
/// and `session/honest-never-punished` held, and each act's frame gets its
/// label with the matching `<label>-closes` held, applied. No run fails.
///
/// This fails if a saved Plan's end record changes. After a deliberate
/// change, regenerate them with `regenerate_saved_plans`.
#[test]
fn saved_plans_replay_their_end_records() {
    for plan in SavedPlan::ALL {
        let (input, pinned) = committed(plan);
        assert_eq!(
            HeaderWire::draw(&input[..2]),
            plan.scenario(),
            "{}",
            plan.name()
        );
        let (report, end, ports) = replay(&input);
        assert_eq!(
            report.end.harness,
            None,
            "{}: {:?}",
            plan.name(),
            report.end
        );
        assert_eq!(
            report.end.violation,
            None,
            "{}: {:?}",
            plan.name(),
            report.end
        );
        let held = |id: &str| {
            let outcome = report
                .properties
                .iter()
                .find(|property| property.property == id)
                .map(|property| &property.outcome);
            assert!(
                matches!(outcome, Some(PropertyOutcome::Held { applicable }) if *applicable > 0),
                "{}: {id} is {outcome:?}",
                plan.name()
            );
        };
        match plan {
            SavedPlan::HonestSync => {
                held(NEVER_CLOSES);
                held(HONEST_NEVER_PUNISHED);
                assert_eq!(ports.selected_tip(), Chain::main(120).tip(&ports.fixture));
            }
            SavedPlan::Act(act) => {
                let row = if act.label() == Label::OverCredit {
                    "subscribe-headers"
                } else {
                    act.row().name()
                };
                let labelled = report.log.entries().iter().any(|entry| {
                    matches!(&entry.obs, Observation::PeerFrame { label, .. } if *label == act.label())
                });
                assert!(labelled, "{}: no frame got {:?}", plan.name(), act.label());
                held(&judging(act.label(), row));
            }
        }
        assert_eq!(
            end,
            pinned,
            "{}: the saved Plan's end record changed; regenerate the saved Plans if that's intended",
            plan.name()
        );
    }
}

/// Rewrites every saved Plan: its input, its sidecar and its end record.
/// The honest sync's Plan is the first of M5.1's 64 inputs, and each act's
/// is the default Plan, the one M5-E1's action test runs. Run it with
/// `RUSTFLAGS="--cfg zakura_sim" cargo test -p zakura-sim --test
/// header_wire_lifecycle -- --ignored regenerate_saved_plans`.
#[test]
#[ignore = "rewrites the committed saved Plans"]
fn regenerate_saved_plans() {
    for plan in SavedPlan::ALL {
        let bytes = match plan {
            SavedPlan::HonestSync => match inputs(1, 16_384).remove(0) {
                PlanInput::Bytes(bytes) => bytes,
                _ => unreachable!("inputs are bytes"),
            },
            SavedPlan::Act(_) => Vec::new(),
        };
        let input: Vec<u8> = plan.tag().into_iter().chain(bytes).collect();
        let (_, end, _) = replay(&input);
        let base = fixtures().join(format!("header-wire-{}", plan.name()));
        fs::write(base.with_extension("bin"), &input).expect("the fixture directory exists");
        let digest: String = Sha256::digest(&input)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let sidecar = format!(
            "# The committed fuzz input `header-wire-{name}.bin`: the harness that\n\
             # runs it, the generator version it was built for, and its SHA-256. Its\n\
             # first two bytes pick the saved `{name}` scenario, and\n\
             # `header-wire-{name}.end.json` is its pinned end record.\n\
             harness = \"{harness}\"\n\
             generator = {generator}\n\
             sha256 = \"{digest}\"\n",
            name = plan.name(),
            harness = HeaderWire::NAME,
            generator = HeaderWire::GENERATOR,
        );
        fs::write(base.with_extension("toml"), sidecar).expect("the fixture directory exists");
        fs::write(base.with_extension("end.json"), format!("{end}\n"))
            .expect("the fixture directory exists");
    }
}
