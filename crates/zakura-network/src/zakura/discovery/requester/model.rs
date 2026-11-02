//! The independent models of [`QueryState`](super::QueryState) and of the settlement
//! ledger in [`Queries`](super::Queries), shared by their proptests and their
//! cargo-fuzz target. Each model restates the documented rules and never calls the
//! code it checks.

use super::*;
use crate::zakura::{
    discovery::protocol::ZakuraServiceId, framed_channel, regulation::PrecheckSlot, CloseCause,
};
use futures::FutureExt;
use std::collections::VecDeque;
use zakura_rt::collections::{hash_map::Entry, HashMap, HashSet};

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

/// The model's view of one live query.
#[derive(Clone, Debug)]
struct QueryModel {
    asked: Asked,
    queued: bool,
    /// Distinguishes this reservation from earlier ones of its kind.
    reservation: usize,
}

/// The largest answer the model allows for `asked`, restated from the
/// wire caps.
fn model_cap(asked: &Asked) -> u64 {
    match asked {
        Asked::Peers { limit } => 2 + u64::from(*limit) * 648,
        Asked::Services(query) => {
            let wanted = query.wanted_services.len();
            let count = if wanted == 0 { 8 } else { wanted.min(8) };
            42 + u64::try_from(count).expect("eight summaries fit u64") * 294
        }
    }
}

const PROPERTY: &str = "discovery/query-ownership";

/// Applicable operations in a decoder-v1 query history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct DiscoveryQueryReplaySummary {
    /// Operations checked against the model.
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

/// Replay decoder-v1 bytes against the query ownership model.
/// Each byte chooses a kind, operation and bounded size. Read at most 512 bytes.
pub fn replay_discovery_query_bytes(data: &[u8]) -> DiscoveryQueryReplaySummary {
    run(decode(data), |_| {})
}

pub(super) fn decode(data: &[u8]) -> impl Iterator<Item = QueryOp> + '_ {
    data.iter().take(512).map(|byte| {
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

pub(super) fn run(
    ops: impl IntoIterator<Item = QueryOp>,
    mut after_step: impl FnMut(&mut QueryState),
) -> DiscoveryQueryReplaySummary {
    let mut state = QueryState::default();
    let mut model: HashMap<QueryKind, QueryModel> = HashMap::new();
    let mut reservations = 0usize;
    let mut admitted = HashSet::new();
    let mut summary = DiscoveryQueryReplaySummary::default();
    for op in ops {
        summary.transitions += 1;
        match op {
            QueryOp::Reserve { kind, size } => {
                let asked = asked_for(kind, size);
                let expected = match model.entry(kind) {
                    Entry::Occupied(_) => Err(QueryLive),
                    Entry::Vacant(slot) => {
                        reservations += 1;
                        slot.insert(QueryModel {
                            asked: asked.clone(),
                            queued: false,
                            reservation: reservations,
                        });
                        Ok(())
                    }
                };
                summary.reserved += usize::from(expected.is_ok());
                summary.refused += usize::from(expected.is_err());
                assert_eq!(state.reserve(asked), expected, "{PROPERTY}");
            }
            QueryOp::Queued(kind) => {
                state.queued(kind);
                if let Some(query) = model.get_mut(&kind) {
                    query.queued = true;
                }
            }
            QueryOp::ReleaseUnsent(kind) => {
                let expected = model.get(&kind).is_some_and(|query| !query.queued);
                if expected {
                    model.remove(&kind);
                }
                summary.released += usize::from(expected);
                assert_eq!(state.release_unsent(kind), expected, "{PROPERTY}");
            }
            QueryOp::Claim { kind, excess } => {
                let expected = model.remove(&kind);
                let payload_len = match (&expected, excess) {
                    (Some(query), Some(excess)) => model_cap(&query.asked) + excess,
                    _ => 0,
                };
                let claimed = state.claim(
                    kind,
                    usize::try_from(payload_len).expect("test caps fit usize"),
                );
                match (expected, claimed) {
                    (None, None) => {
                        summary.stale += 1;
                    }
                    (Some(query), Some(Ok(answer))) => {
                        summary.admitted += 1;
                        assert!(payload_len <= model_cap(&query.asked), "{PROPERTY}");
                        assert_eq!(answer.kind(), kind, "{PROPERTY}");
                        assert_eq!(&answer.asked, &query.asked, "{PROPERTY}");
                        assert!(
                            admitted.insert(query.reservation),
                            "{PROPERTY}: duplicate admission"
                        );
                    }
                    (Some(query), Some(Err(refused))) => {
                        summary.oversized += 1;
                        let cap = model_cap(&query.asked);
                        assert!(payload_len > cap, "{PROPERTY}");
                        assert_eq!(
                            refused,
                            ClaimRefused::OverBytes { bytes: cap },
                            "{PROPERTY}"
                        );
                    }
                    (expected, claimed) => {
                        panic!("{PROPERTY}: model {expected:?} disagrees with claim {claimed:?}")
                    }
                }
            }
        }

        after_step(&mut state);
        assert_eq!(state.len(), model.len(), "{PROPERTY}");
        assert!(state.len() <= QUERY_KINDS.len(), "{PROPERTY}");
        for (kind, query) in &model {
            let live = state
                .live
                .get(kind)
                .expect("the model's live query is live");
            assert_eq!(&live.asked, &query.asked, "{PROPERTY}");
            assert_eq!(live.phase == QueryPhase::Queued, query.queued, "{PROPERTY}");
        }
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
    /// Operations checked against the model.
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

/// The model's view of one live query.
struct SettlementQuery {
    asked: Asked,
    written: bool,
    /// The payload length of the answer the transport admitted.
    delivered: Option<usize>,
}

/// One session under test, and the model's view of it.
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
        let fence = WriterFence::new(connection.clone(), CloseCause::new());
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
    /// close wait exactly when the model says they do.
    fn check_close_wait(&self) {
        let holds = !self.answers.is_empty()
            || self.hello.is_some()
            || self.live.values().any(|query| query.delivered.is_some());
        assert_eq!(
            self.queries.answers_settled().now_or_never().is_none(),
            holds,
            "{SETTLEMENT}: the close wait disagrees with the model"
        );
    }

    /// The session's ledger matches the model. A session holds at most one
    /// answer of each kind.
    fn check_ledger(&self) {
        let settling = *self.queries.settling.borrow();
        assert_eq!(
            settling,
            Settling {
                peers: self.answers_of(QueryKind::Peers),
                services: self.answers_of(QueryKind::Services),
                hellos: usize::from(self.hello.is_some()),
            },
            "{SETTLEMENT}"
        );
        assert!(
            settling.peers <= 1 && settling.services <= 1,
            "{SETTLEMENT}: a kind stacked two answers"
        );
        for kind in QUERY_KINDS {
            assert!(
                !(self.live.contains_key(&kind) && settling.holds(kind)),
                "{SETTLEMENT}: a query is live beside its kind's unsettled answer"
            );
        }
        let live = self.queries.lock();
        assert_eq!(live.state.len(), self.live.len(), "{SETTLEMENT}");
        assert_eq!(live.reservations.len(), self.live.len(), "{SETTLEMENT}");
    }
}

/// The most entries the driver's pool holds: enough for the current
/// session's two queries, and one unsettled answer beside them.
const SETTLEMENT_POOL: usize = 3;

/// The largest answer that the transport admits for a kind's row, restated
/// from the wire caps.
fn model_row_cap(kind: QueryKind) -> u64 {
    match kind {
        QueryKind::Peers => 2 + 32 * 648,
        QueryKind::Services => 42 + 8 * 294,
    }
}

fn answer_type(kind: QueryKind) -> u16 {
    u16::from(match kind {
        QueryKind::Peers => MSG_DISCOVERY_PEERS,
        QueryKind::Services => MSG_DISCOVERY_SERVICES,
    })
}

/// Replay decoder-v2 bytes against the settlement model. Each byte chooses a
/// kind, an operation, and a bounded argument. Read at most 512 bytes.
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
                let due = !session.live.contains_key(&kind)
                    && session.answers_of(kind) == 0
                    && held < SETTLEMENT_POOL;
                // A `GetPeers` limit of zero asks for nothing, so sizes
                // start at one.
                let asked = asked_for(kind, size + usize::from(kind == QueryKind::Peers));
                match (session.send(asked.clone()), due) {
                    (Ok(()), true) => {
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
                    (Err(QueryUnsent::NotDue), false) => summary.not_due += 1,
                    (sent, due) => {
                        panic!("{SETTLEMENT}: due {due} disagrees with send {sent:?}")
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
                        panic!("{SETTLEMENT}: write {frame:?} disagrees with model {kind:?}")
                    }
                }
            }
            SettlementOp::Deliver { kind, excess } => {
                let query = session.live.get(&kind);
                let len = query.map_or(2, |query| model_cap(&query.asked) + u64::from(excess));
                let checked = session
                    .precheck
                    .get()
                    .expect("the session attached its precheck")
                    .check(
                        answer_type(kind),
                        usize::try_from(len).expect("test caps fit usize"),
                    );
                let session_query = session.live.get_mut(&kind);
                match (session_query, checked) {
                    (None, Err(FrameRejection::Unsolicited)) => summary.refused += 1,
                    (Some(_), Err(FrameRejection::AboveReservation { .. }))
                        if len > model_row_cap(kind) =>
                    {
                        summary.refused += 1;
                    }
                    (Some(query), Err(FrameRejection::AboveOwed { .. }))
                        if query.delivered.is_some() =>
                    {
                        summary.refused += 1;
                    }
                    (Some(query), Ok(()))
                        if query.delivered.is_none() && len <= model_row_cap(kind) =>
                    {
                        summary.delivered += 1;
                        query.delivered = Some(usize::try_from(len).expect("test caps fit usize"));
                    }
                    (query, checked) => panic!(
                        "{SETTLEMENT}: live {} disagrees with admission {checked:?}",
                        query.is_some()
                    ),
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
                    let cap = model_cap(&query.asked);
                    match session.queries.claim(answer_type(kind), len) {
                        Ok(Some(answer)) if len as u64 <= cap => {
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
