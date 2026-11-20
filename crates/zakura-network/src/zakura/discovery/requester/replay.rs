//! The `discovery_query` machine target: histories through
//! [`QueryState`](super::QueryState) under decoder 1, and through one node's
//! [`Queries`](super::Queries) sessions under decoder 2, shared by their
//! proptests and their cargo-fuzz target.
//!
//! Each replay drives the production code as the session's writer, the
//! transport reader and the session reader do (see
//! [`machine_test`](crate::zakura::machine_test)). It checks the invariants
//! after every operation, and that retiring every session returns every pool
//! entry. It compares each result only with what the replay sent and holds:
//! which queries are live, which it wrote, which answers it delivered, claimed
//! and holds, and each query's own cap. It never re-derives when a query is
//! due or which frames the transport admits. Named scenarios in `tests.rs` pin
//! those rules.

use super::*;
use crate::zakura::{
    discovery::protocol::ZakuraServiceId, framed_channel, machine_test::MAX_HISTORY_BYTES,
    regulation::PrecheckSlot,
};
use futures::FutureExt;
use std::collections::VecDeque;
use zakura_rt::collections::{HashMap, HashSet};

pub(super) fn get_peers(limit: u16) -> GetPeers {
    GetPeers {
        limit,
        wanted_services: Vec::new(),
        exclude_node_ids: Vec::new(),
    }
}

pub(super) fn get_services(wanted_services: Vec<ZakuraServiceId>) -> GetServices {
    GetServices { wanted_services }
}

/// One step of a live-query operation sequence.
#[derive(Clone, Debug)]
pub(super) enum QueryOp {
    /// Reserve a query of `kind`, sized by `size`.
    Reserve {
        kind: QueryKind,
        size: usize,
    },
    Queued(QueryKind),
    ReleaseUnsent(QueryKind),
    /// Claim `kind` with an answer `excess` bytes above its query's cap,
    /// or empty when `excess` is `None`.
    Claim {
        kind: QueryKind,
        excess: Option<u64>,
    },
}

/// The query a `Reserve` step asks. `size` is a `GetPeers` limit or the
/// length of a `GetServices` wanted list.
fn asked_for(kind: QueryKind, size: usize) -> Asked {
    let wanted = [
        ZakuraServiceId::discovery(),
        ZakuraServiceId::header_sync(),
        ZakuraServiceId::block_sync(),
    ];
    match kind {
        QueryKind::Peers => Asked::Peers {
            limit: u16::try_from(size).expect("test sizes are small"),
        },
        QueryKind::Services => Asked::Services(get_services(wanted[..size.min(3)].to_vec())),
    }
}

/// One live query, as the replay saw `reserve` and `queued` take it.
#[derive(Clone, Debug)]
struct Live {
    asked: Asked,
    queued: bool,
    /// Distinguishes this reservation from earlier ones of its kind.
    reservation: usize,
}

const PROPERTY: &str = "discovery/query-ownership";

/// Applicable operations in a decoder-v1 query history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct DiscoveryQueryReplaySummary {
    /// Operations applied to the state.
    pub transitions: usize,
    /// Fresh reservations.
    pub reserved: usize,
    /// Reservations refused because the kind is live.
    pub refused: usize,
    /// Unsent reservations released.
    pub released: usize,
    /// Answers admitted by their own reservation.
    pub admitted: usize,
    /// Answers rejected above their reservation's cap.
    pub oversized: usize,
    /// Claims without a live reservation.
    pub stale: usize,
}

/// Replay decoder-v1 bytes through [`QueryState`]. Each byte chooses a kind,
/// an operation and a bounded size. Read at most [`MAX_HISTORY_BYTES`] bytes.
pub fn replay_discovery_query_bytes(data: &[u8]) -> DiscoveryQueryReplaySummary {
    run(decode(data), |_| {})
}

pub(super) fn decode(data: &[u8]) -> impl Iterator<Item = QueryOp> + '_ {
    data.iter().take(MAX_HISTORY_BYTES).map(|byte| {
        let kind = if byte & 1 == 0 {
            QueryKind::Peers
        } else {
            QueryKind::Services
        };
        match (byte >> 1) % 4 {
            0 => QueryOp::Reserve {
                kind,
                size: usize::from(byte >> 3) % 4,
            },
            1 => QueryOp::Queued(kind),
            2 => QueryOp::ReleaseUnsent(kind),
            _ => QueryOp::Claim {
                kind,
                excess: match (byte >> 3) % 3 {
                    0 => None,
                    1 => Some(0),
                    _ => Some(1),
                },
            },
        }
    })
}

/// Drives `ops` through a fresh [`QueryState`], and calls `after_step` after
/// each operation, so a test can plant a fault.
pub(super) fn run(
    ops: impl IntoIterator<Item = QueryOp>,
    mut after_step: impl FnMut(&mut QueryState),
) -> DiscoveryQueryReplaySummary {
    let mut state = QueryState::default();
    // The live queries, as the state's results report them.
    let mut live: HashMap<QueryKind, Live> = HashMap::new();
    let mut reservations = 0usize;
    let mut admitted = HashSet::new();
    let mut summary = DiscoveryQueryReplaySummary::default();
    for op in ops {
        summary.transitions += 1;
        match op {
            QueryOp::Reserve { kind, size } => {
                let asked = asked_for(kind, size);
                let reserved = state.reserve(asked.clone());
                assert_eq!(
                    reserved.is_err(),
                    live.contains_key(&kind),
                    "{PROPERTY}: one live query of each kind"
                );
                if reserved.is_ok() {
                    reservations += 1;
                    live.insert(
                        kind,
                        Live {
                            asked,
                            queued: false,
                            reservation: reservations,
                        },
                    );
                    summary.reserved += 1;
                } else {
                    summary.refused += 1;
                }
            }
            QueryOp::Queued(kind) => {
                state.queued(kind);
                if let Some(query) = live.get_mut(&kind) {
                    query.queued = true;
                }
            }
            QueryOp::ReleaseUnsent(kind) => {
                let released = state.release_unsent(kind);
                let unsent = live.get(&kind).is_some_and(|query| !query.queued);
                assert_eq!(
                    released, unsent,
                    "{PROPERTY}: only an unsent query is released"
                );
                if released {
                    live.remove(&kind);
                    summary.released += 1;
                }
            }
            QueryOp::Claim { kind, excess } => {
                let query = live.remove(&kind);
                let cap = query.as_ref().map(|query| query.asked.cap().bytes);
                let payload_len = match (cap, excess) {
                    (Some(cap), Some(excess)) => cap + excess,
                    _ => 0,
                };
                let claimed = state.claim(
                    kind,
                    usize::try_from(payload_len).expect("test caps fit usize"),
                );
                match (query, claimed) {
                    (None, None) => summary.stale += 1,
                    (Some(query), Some(Ok(answer))) => {
                        summary.admitted += 1;
                        assert!(payload_len <= query.asked.cap().bytes, "{PROPERTY}");
                        assert_eq!(answer.kind(), kind, "{PROPERTY}");
                        assert_eq!(&answer.asked, &query.asked, "{PROPERTY}");
                        assert!(
                            admitted.insert(query.reservation),
                            "{PROPERTY}: duplicate admission"
                        );
                    }
                    (Some(query), Some(Err(refused))) => {
                        summary.oversized += 1;
                        let cap = query.asked.cap().bytes;
                        assert!(payload_len > cap, "{PROPERTY}");
                        assert_eq!(
                            refused,
                            ClaimRefused::OverBytes { bytes: cap },
                            "{PROPERTY}"
                        );
                    }
                    (query, claimed) => panic!(
                        "{PROPERTY}: live {} disagrees with claim {claimed:?}",
                        query.is_some()
                    ),
                }
            }
        }
        after_step(&mut state);
        state.assert_invariants();
    }
    summary
}

const SETTLEMENT: &str = "discovery/answer-settlement";

/// The most operations one decoder-v2 history runs.
pub(super) const SETTLEMENT_MAX_OPS: usize = 512;

/// One step of a decoder-v2 settlement history.
#[derive(Clone, Copy, Debug)]
pub(super) enum SettlementOp {
    /// Send a query of `kind`, sized by `size`.
    Query { kind: QueryKind, size: usize },
    /// The transport writes the oldest queued query.
    Write,
    /// The transport admits an answer to `kind`, at its query's cap or one
    /// byte above it.
    Deliver { kind: QueryKind, excess: bool },
    /// The reader takes the admitted answer to `kind` and claims its query.
    Claim(QueryKind),
    /// An unsettled answer of `kind` settles, in the current session or a
    /// retired one.
    Settle { kind: QueryKind, retired: bool },
    /// The reader starts verifying a `Hello`.
    HelloBegin,
    /// The `Hello` in verification settles.
    HelloEnd,
    /// The session ends and a new session starts on a new connection, on
    /// the same pool. A reader can outlive its session's retirement, so a
    /// claimed answer stays unsettled.
    Retire,
}

pub(super) fn decode_settlement(data: &[u8]) -> impl Iterator<Item = SettlementOp> + '_ {
    data.iter().take(SETTLEMENT_MAX_OPS).map(|byte| {
        let kind = if byte & 1 == 0 {
            QueryKind::Peers
        } else {
            QueryKind::Services
        };
        let argument = byte >> 4;
        match (byte >> 1) % 8 {
            0 => SettlementOp::Query {
                kind,
                size: usize::from(argument) % 4,
            },
            1 => SettlementOp::Write,
            2 => SettlementOp::Deliver {
                kind,
                excess: argument & 1 == 1,
            },
            3 => SettlementOp::Claim(kind),
            4 => SettlementOp::Settle {
                kind,
                retired: argument & 1 == 1,
            },
            5 => SettlementOp::HelloBegin,
            6 => SettlementOp::HelloEnd,
            _ => SettlementOp::Retire,
        }
    })
}

/// Applicable operations in a decoder-v2 settlement history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct DiscoverySettlementReplaySummary {
    /// Operations applied to the session.
    pub transitions: usize,
    /// Queries sent.
    pub sent: usize,
    /// Queries that were not due: their kind was live or unsettled, or the
    /// pool was full.
    pub not_due: usize,
    /// Queries the transport wrote.
    pub written: usize,
    /// Answers the transport admitted.
    pub delivered: usize,
    /// Answers the transport refused.
    pub refused: usize,
    /// Answers that claimed their query and began settling.
    pub claimed: usize,
    /// Admitted answers that the claim refused above their query's cap.
    pub oversized: usize,
    /// Answers that settled.
    pub settled: usize,
    /// `Hello` holds taken.
    pub hellos: usize,
    /// Operations that had nothing to act on.
    pub stale: usize,
    /// Sessions retired.
    pub retired: usize,
    /// Retirements that closed their connection.
    pub closed: usize,
}

/// One query the replay sent and has not seen claimed.
struct SettlementQuery {
    asked: Asked,
    written: bool,
    /// The payload length of the answer the transport admitted.
    delivered: Option<usize>,
}

/// One session under test, and what the replay sent and holds on it.
struct SettlementSession {
    queries: Queries,
    /// The transport's precheck slot for this session's answers.
    precheck: PrecheckSlot,
    /// The peer's end of the stream that carries this session's queries.
    written: FramedRecv,
    connection: CancellationToken,
    /// Keeps the answer stream's channel open.
    _answer_stream: (FramedSend, FramedRecv),
    live: HashMap<QueryKind, SettlementQuery>,
    /// The kinds of the queued queries that a claim has not abandoned.
    unwritten: VecDeque<QueryKind>,
    answers: Vec<ClaimedAnswer>,
    hello: Option<SettlingHold>,
}

impl SettlementSession {
    fn new(pool: &ReservationPool) -> Self {
        let precheck = PrecheckSlot::default();
        let (answer_send, answer_recv) = framed_channel(1);
        let answer_recv = answer_recv.with_precheck(precheck.clone());
        let (send, written) = framed_channel(SETTLEMENT_MAX_OPS);
        let connection = CancellationToken::new();
        let fence = WriterFence::new(connection.clone());
        let queries = Queries::new(
            pool.clone(),
            fence,
            &answer_recv,
            send,
            CancellationToken::new(),
        );
        Self {
            queries,
            precheck,
            written,
            connection,
            _answer_stream: (answer_send, answer_recv),
            live: HashMap::new(),
            unwritten: VecDeque::new(),
            answers: Vec::new(),
            hello: None,
        }
    }

    fn answers_of(&self, kind: QueryKind) -> usize {
        self.answers
            .iter()
            .filter(|answer| answer.admitted().kind() == kind)
            .count()
    }

    /// Sends the query, whose future has queue room and never waits.
    fn send(&self, asked: Asked) -> Result<(), QueryUnsent> {
        let sending = match asked {
            Asked::Peers { limit } => self.queries.send_get_peers(get_peers(limit)).left_future(),
            Asked::Services(query) => self.queries.send_get_services(query).right_future(),
        };
        sending
            .now_or_never()
            .expect("a query waits only for queue room, and the queue holds every operation")
    }

    /// The session's claimed answers, `Hello`, and queued answers hold the
    /// close wait, and nothing else does.
    fn check_close_wait(&self) {
        let holds = !self.answers.is_empty()
            || self.hello.is_some()
            || self.live.values().any(|query| query.delivered.is_some());
        assert_eq!(
            self.queries.answers_settled().now_or_never().is_none(),
            holds,
            "{SETTLEMENT}: the close wait disagrees with what the session holds"
        );
    }

    /// The session's ledger keeps its invariants and holds a reservation for
    /// each query the replay sent and has not seen claimed.
    fn check_ledger(&self) {
        self.queries.assert_invariants();
        assert_eq!(
            self.queries.lock().reservations.len(),
            self.live.len(),
            "{SETTLEMENT}: reservations"
        );
    }
}

/// The most entries the driver's pool holds: enough for the current
/// session's two queries, and one unsettled answer beside them.
const SETTLEMENT_POOL: usize = 3;

/// The largest answer that the transport admits for a kind's row.
fn row_cap(kind: QueryKind) -> u64 {
    match kind {
        QueryKind::Peers => LARGEST_PEERS.bytes,
        QueryKind::Services => LARGEST_SERVICES.bytes,
    }
}

fn answer_type(kind: QueryKind) -> u16 {
    u16::from(match kind {
        QueryKind::Peers => MSG_DISCOVERY_PEERS,
        QueryKind::Services => MSG_DISCOVERY_SERVICES,
    })
}

/// Replay decoder-v2 bytes through one node's [`Queries`] sessions. Each byte
/// chooses a kind, an operation, and a bounded argument. Read at most
/// [`SETTLEMENT_MAX_OPS`] bytes.
pub fn replay_discovery_settlement_bytes(data: &[u8]) -> DiscoverySettlementReplaySummary {
    run_settlement(decode_settlement(data), |_, _| {})
}

pub(super) fn run_settlement(
    ops: impl IntoIterator<Item = SettlementOp>,
    mut after_step: impl FnMut(&Queries, &ReservationPool),
) -> DiscoverySettlementReplaySummary {
    let pool = ReservationPool::new(SETTLEMENT_POOL).expect("the pool size is valid");
    let mut session = SettlementSession::new(&pool);
    // Retired sessions that still hold unsettled answers.
    let mut retired: Vec<SettlementSession> = Vec::new();
    let mut summary = DiscoverySettlementReplaySummary::default();

    for op in ops {
        summary.transitions += 1;
        match op {
            SettlementOp::Query { kind, size } => {
                let held = pool.held();
                // A `GetPeers` limit of zero asks for nothing, so sizes
                // start at one.
                let asked = asked_for(kind, size + usize::from(kind == QueryKind::Peers));
                match session.send(asked.clone()) {
                    Ok(()) => {
                        // A query is sent only beside no live query or
                        // unsettled answer of its kind, into a pool with room.
                        assert!(
                            !session.live.contains_key(&kind)
                                && session.answers_of(kind) == 0
                                && held < SETTLEMENT_POOL,
                            "{SETTLEMENT}: a query sent while not due"
                        );
                        summary.sent += 1;
                        session.live.insert(
                            kind,
                            SettlementQuery {
                                asked,
                                written: false,
                                delivered: None,
                            },
                        );
                        session.unwritten.push_back(kind);
                    }
                    Err(QueryUnsent::NotDue) => summary.not_due += 1,
                    Err(QueryUnsent::Closed) => {
                        panic!("{SETTLEMENT}: the current session refused a query as closed")
                    }
                }
            }
            SettlementOp::Write => {
                match (session.written.try_recv(), session.unwritten.pop_front()) {
                    (Ok(frame), Some(kind)) => {
                        summary.written += 1;
                        let query = session
                            .live
                            .get_mut(&kind)
                            .expect("an unwritten frame's query is live");
                        assert_eq!(
                            frame.message_type,
                            query.asked.request_type(),
                            "{SETTLEMENT}"
                        );
                        query.written = true;
                    }
                    (Err(_), None) => summary.stale += 1,
                    (frame, kind) => {
                        panic!("{SETTLEMENT}: write {frame:?} disagrees with the queued {kind:?}")
                    }
                }
            }
            SettlementOp::Deliver { kind, excess } => {
                let query = session.live.get(&kind);
                let len = query.map_or(2, |query| query.asked.cap().bytes + u64::from(excess));
                let checked = session
                    .precheck
                    .get()
                    .expect("the session attached its precheck")
                    .check(
                        answer_type(kind),
                        usize::try_from(len).expect("test caps fit usize"),
                    );
                if checked.is_ok() {
                    // The transport queues only an answer that a live query
                    // owes, at most one per query, and within its row.
                    let query = session
                        .live
                        .get_mut(&kind)
                        .unwrap_or_else(|| panic!("{SETTLEMENT}: admitted an unsolicited answer"));
                    assert!(
                        query.delivered.is_none(),
                        "{SETTLEMENT}: admitted a second answer"
                    );
                    assert!(
                        len <= row_cap(kind),
                        "{SETTLEMENT}: admitted {len} bytes above the row's cap"
                    );
                    summary.delivered += 1;
                    query.delivered = Some(usize::try_from(len).expect("test caps fit usize"));
                } else {
                    summary.refused += 1;
                }
            }
            SettlementOp::Claim(kind) => {
                // The reader sees only the frames the transport admitted.
                let delivered = session.live.get(&kind).and_then(|query| query.delivered);
                if let Some(len) = delivered {
                    let query = session
                        .live
                        .remove(&kind)
                        .expect("the delivered answer's query is live");
                    session.unwritten.retain(|unwritten| *unwritten != kind);
                    let cap = query.asked.cap().bytes;
                    match session.queries.claim(answer_type(kind), len) {
                        Ok(Claim::Answer(answer)) if len as u64 <= cap => {
                            summary.claimed += 1;
                            assert_eq!(answer.admitted().kind(), kind, "{SETTLEMENT}");
                            session.answers.push(answer);
                        }
                        Err(ClaimRefused::OverBytes { bytes })
                            if len as u64 > cap && bytes == cap =>
                        {
                            summary.oversized += 1;
                        }
                        claimed => {
                            panic!("{SETTLEMENT}: length {len} above cap {cap} got {claimed:?}")
                        }
                    }
                } else {
                    summary.stale += 1;
                }
            }
            SettlementOp::Settle {
                kind,
                retired: settle_retired,
            } => {
                let holder = if settle_retired {
                    retired.iter_mut().find(|old| old.answers_of(kind) > 0)
                } else {
                    Some(&mut session)
                };
                let position = holder.and_then(|holder| {
                    holder
                        .answers
                        .iter()
                        .position(|answer| answer.admitted().kind() == kind)
                        .map(|position| (holder, position))
                });
                if let Some((holder, position)) = position {
                    summary.settled += 1;
                    drop(holder.answers.remove(position));
                } else {
                    summary.stale += 1;
                }
                retired.retain(|old| !old.answers.is_empty());
            }
            SettlementOp::HelloBegin => {
                // The reader handles one frame at a time.
                if session.hello.is_none() {
                    summary.hellos += 1;
                    session.hello = Some(session.queries.verifying_hello());
                } else {
                    summary.stale += 1;
                }
            }
            SettlementOp::HelloEnd => {
                if session.hello.take().is_none() {
                    summary.stale += 1;
                }
            }
            SettlementOp::Retire => {
                summary.retired += 1;
                let closes = session.live.values().any(|query| query.written);
                session.queries.retire();
                assert_eq!(session.connection.is_cancelled(), closes, "{SETTLEMENT}");
                summary.closed += usize::from(closes);
                let mut ended = std::mem::replace(&mut session, SettlementSession::new(&pool));
                ended.live.clear();
                ended.unwritten.clear();
                ended.hello = None;
                if !ended.answers.is_empty() {
                    retired.push(ended);
                }
            }
        }

        after_step(&session.queries, &pool);
        let unsettled: usize = std::iter::once(&session)
            .chain(&retired)
            .map(|held| held.answers.len())
            .sum();
        assert_eq!(
            pool.held(),
            session.live.len() + unsettled,
            "{SETTLEMENT}: each live query and unsettled answer holds one pool entry"
        );
        for held in std::iter::once(&session).chain(&retired) {
            held.check_ledger();
            held.check_close_wait();
        }
    }

    session.queries.retire();
    drop(session);
    drop(retired);
    assert_eq!(
        pool.held(),
        0,
        "{SETTLEMENT}: a settled history holds nothing"
    );
    summary
}
