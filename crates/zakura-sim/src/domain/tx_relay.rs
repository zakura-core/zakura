//! `tx-relay`'s domain properties: its DoS bounds and its ordering rules.
//!
//! The node's routines report each step through `tx-relay`'s simulator hook
//! ([`TxRelayEvent`]), under the node's lock, so the steps of all sessions
//! arrive in the order they happened. The harness's test pool reports each
//! id it admits, and the scripted peers report each close-table row they
//! break and each node state frame out of order. All of it lands in one
//! [`RelayEvidence`], which judges each fact as it arrives: a fact is about
//! a moment inside the run. The closes the node decides come from the
//! transport tap's log.
//!
//! Each property's bound or rule comes from production's constants
//! ([`MAX_REQUESTS_PER_PEER`], [`MAX_SERVES`], [`PENDING_PER_PEER`],
//! [`KNOWN_CAP`]), which the hook re-exports, and its wording from the
//! `tx-relay` rows of the property catalogue.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use zakura_chain::transaction::UnminedTxId;
use zakura_network::zakura::{
    tx_relay_sim::{
        Received, SimEffect, SimState, TxRelayEvent, KNOWN_CAP, MAX_REQUESTS_PER_PEER, MAX_SERVES,
        PENDING_PER_PEER,
    },
    CloseOutcome, CloseRow, NeutralCause, ZakuraPeerId,
};

use crate::{
    observe::TransportFact,
    peer::{peer_id, peer_of},
    property::{
        CheckCtx, HasView, Kind, Property, PropertyId, Source, Status, TriggerHistory, Verdict,
    },
    Observation, ObservationLog,
};

/// Ids reserving, requested or awaiting a verdict stay within
/// [`MAX_REQUESTS_PER_PEER`] per peer, and requests within the peer's
/// `Hello`.
pub const REQUESTS_WITHIN_LIMIT: &str = "tx-relay/requests-within-limit";
/// The peer's requests the node holds unanswered stay within
/// [`MAX_SERVES`].
pub const SERVING_WITHIN_LIMIT: &str = "tx-relay/serving-within-limit";
/// A session holds at most [`PENDING_PER_PEER`] ids for its peer.
pub const PENDING_WITHIN_BOUND: &str = "tx-relay/pending-within-bound";
/// A session's skip set holds at most [`KNOWN_CAP`] ids.
pub const SKIP_SET_WITHIN_BOUND: &str = "tx-relay/skip-set-within-bound";
/// A body reaches the pool only for a request outstanding on its session,
/// under the requested id and at the announced size.
pub const REQUESTED_BODIES_ONLY: &str = "tx-relay/requested-bodies-only";
/// A cooling peer's bodies never reach the pool.
pub const COOLING_BODIES_DROPPED: &str = "tx-relay/cooling-bodies-dropped";
/// A first stall parks the session, and only a stall in the session the
/// park readmits closes the connection.
pub const STALLED_PEERS_PARKED: &str = "tx-relay/stalled-peers-parked";
/// The node's `Hello` goes first, once, and it asks nothing before the
/// peer's `Hello`.
pub const HELLO_FIRST: &str = "tx-relay/hello-first";
/// The node serves an id only on a session it announced that id on.
pub const NO_PROBE: &str = "tx-relay/no-probe";
/// The node announces only ids its pool admitted, and never back to a
/// session that announced or supplied them.
pub const ANNOUNCE_AFTER_ADMISSION: &str = "tx-relay/announce-after-admission";
/// A `tx-relay` fault close names a row the peer broke on that connection.
pub const FAULT_CLOSE_NAMES_A_BROKEN_ROW: &str = "tx-relay/fault-close-names-a-broken-row";

/// Every property, in registration order.
pub const IDS: [&str; 11] = [
    REQUESTS_WITHIN_LIMIT,
    SERVING_WITHIN_LIMIT,
    PENDING_WITHIN_BOUND,
    SKIP_SET_WITHIN_BOUND,
    REQUESTED_BODIES_ONLY,
    COOLING_BODIES_DROPPED,
    STALLED_PEERS_PARKED,
    HELLO_FIRST,
    NO_PROBE,
    ANNOUNCE_AFTER_ADMISSION,
    FAULT_CLOSE_NAMES_A_BROKEN_ROW,
];

/// Each property's claim, in [`IDS`] order.
const CLAIMS: [&str; 11] = [
    "a peer's ids reserving, requested or awaiting a verdict stay within 5 across its sessions, and a session's requests within the peer's Hello",
    "the peer's requests the node holds unanswered on a session stay within 16, the Hello the node sends",
    "a session holds at most 8,000 ids for its peer, pending or waiting for the peer's Hello",
    "a session's skip set holds at most 24,000 ids",
    "a body reaches the pool only for a request outstanding on its session, under the requested id and at the size the peer announced",
    "a body from a cooling peer never reaches the pool",
    "a peer's first stall parks its session and keeps its connection; only a stall in the session its park readmits closes the connection",
    "the node's first state frame on a session is its Hello, sent once, and it asks for nothing before the peer's Hello",
    "the node serves an id only on a session it announced that id on",
    "the node announces only ids its pool admitted, and never on a session that announced or supplied them",
    "a tx-relay fault close names a close-table row the peer broke on that connection",
];

/// One property's state.
#[derive(Clone, Debug, Default)]
pub struct Fact {
    /// Whether a step reached a case the property judges.
    pub applied: bool,
    /// Whether the run reached the case the property's witness needs.
    pub triggered: bool,
    /// The first break: what was expected, and what happened.
    pub violation: Option<(String, String)>,
}

/// A transaction id that sorts, by its mined id and then its authorizing
/// data, so the evidence's maps iterate the same way in every run.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxKey(pub UnminedTxId);

impl TxKey {
    /// The bytes it sorts by.
    fn bytes(self) -> ([u8; 32], [u8; 32]) {
        let digest = self.0.auth_digest().map_or([0xFF; 32], |digest| digest.0);
        (self.0.mined_id().0, digest)
    }
}

impl Ord for TxKey {
    /// By the mined id, then the authorizing data.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.bytes().cmp(&other.bytes())
    }
}

impl PartialOrd for TxKey {
    /// See [`Ord`].
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Debug for TxKey {
    /// The mined id's first bytes, in hex.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (id, _) = self.bytes();
        for byte in id.iter().rev().take(6) {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// One node session, as the hook shows it.
#[derive(Clone, Debug, Default)]
struct Session {
    /// The scripted peer, by index, if the node's peer ID is one.
    peer: Option<usize>,
    /// Its peer's park let it in.
    readmitted: bool,
    /// The peer's `Hello` reached the routine.
    hello: bool,
    /// The ids the node asked for on it.
    asked: BTreeSet<TxKey>,
    /// The sizes the peer announced each id with on it.
    sizes: BTreeMap<TxKey, BTreeSet<u32>>,
    /// The ids the peer announced or supplied on it, each with the count of
    /// such ids before it, so an id the skip set evicted is known.
    told: BTreeMap<TxKey, u64>,
    /// Ids the peer announced or supplied on it, so far.
    told_count: u64,
    /// The ids the node announced on it.
    sent: BTreeSet<TxKey>,
    /// Its latest state.
    state: SimState,
}

impl Session {
    /// Whether the peer told the node about `id`, and the skip set still
    /// holds it. A skip set evicts its oldest past [`KNOWN_CAP`], so an id
    /// with that many after it may be gone.
    fn told(&self, id: TxKey) -> bool {
        self.told
            .get(&id)
            .is_some_and(|at| self.told_count - at < KNOWN_CAP as u64)
    }

    /// The peer told the node about `id`.
    fn tell(&mut self, id: TxKey) {
        if !self.told.contains_key(&id) {
            self.told.insert(id, self.told_count);
            self.told_count += 1;
        }
    }
}

/// What the node's steps, the pool and the peers showed, and each
/// property's state.
#[derive(Debug, Default)]
pub struct RelayEvidence {
    /// The scenario's seed, which names each scripted peer's ID.
    pub seed: [u8; 32],
    /// The scripted peers.
    pub peers: usize,
    /// Each property's state, by ID.
    pub facts: BTreeMap<&'static str, Fact>,
    /// The sessions, by the hook's number.
    sessions: BTreeMap<u64, Session>,
    /// Peers a park refused or parked, by index.
    parked: BTreeSet<usize>,
    /// The ids the pool admitted.
    admitted: BTreeSet<TxKey>,
    /// The close-table rows each scripted peer broke, by index.
    broken: BTreeMap<usize, BTreeSet<&'static str>>,
    /// Log entries [`RelayEvidence::read_log`] has read.
    log_read: usize,
    /// Each tap connection's scripted peer, from its registration.
    conn_peers: BTreeMap<u64, usize>,
}

/// The evidence the hook, the pool, the peers and the checks share. Each
/// holds the lock only between preemption points.
pub type SharedRelay = Arc<Mutex<RelayEvidence>>;

/// Locks `evidence`. A check that panicked while it held the lock left
/// the evidence whole, since every write is one field at a time.
pub fn lock(evidence: &SharedRelay) -> MutexGuard<'_, RelayEvidence> {
    evidence.lock().unwrap_or_else(PoisonError::into_inner)
}

impl RelayEvidence {
    /// The state of property `id`.
    fn fact(&mut self, id: &'static str) -> &mut Fact {
        self.facts.entry(id).or_default()
    }

    /// Marks `id` applied.
    fn apply(&mut self, id: &'static str) {
        self.fact(id).applied = true;
    }

    /// Marks `id` applied and its witness's case reached.
    fn trigger(&mut self, id: &'static str) {
        let fact = self.fact(id);
        fact.applied = true;
        fact.triggered = true;
    }

    /// Records `id`'s first break, which reaches its witness's case too.
    fn violate(
        &mut self,
        id: &'static str,
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) {
        let fact = self.fact(id);
        fact.applied = true;
        fact.triggered = true;
        fact.violation
            .get_or_insert_with(|| (expected.into(), actual.into()));
    }

    /// The scripted peer whose node ID is `peer`, by index.
    fn peer_index(&self, peer: &ZakuraPeerId) -> Option<usize> {
        (0..self.peers).find(|index| peer_id(self.seed, *index) == *peer)
    }

    /// The pool admitted `id`. The test pool reports it before it publishes
    /// the change, so before any routine can read it.
    pub fn admitted(&mut self, id: UnminedTxId) {
        self.admitted.insert(TxKey(id));
    }

    /// The scripted peer at `addr` wrote a frame whose content breaks
    /// `row`, before the write.
    pub fn broke(&mut self, addr: SocketAddr, row: CloseRow) {
        if let Some(peer) = peer_of(addr) {
            self.broken.entry(peer).or_default().insert(row.id());
        }
    }

    /// A scripted peer read the node's state frames out of order.
    pub fn node_hello_out_of_order(&mut self, what: String) {
        self.violate(
            HELLO_FIRST,
            "the node's first state frame on a session is its Hello, sent once",
            what,
        );
    }

    /// One step the hook reported.
    pub fn step(&mut self, event: TxRelayEvent) {
        match event {
            TxRelayEvent::Opened {
                session,
                peer,
                readmitted,
                parked,
            } => {
                let index = self.peer_index(&peer);
                if parked {
                    self.parked.extend(index);
                }
                self.sessions.insert(
                    session,
                    Session {
                        peer: index,
                        readmitted,
                        ..Session::default()
                    },
                );
            }
            TxRelayEvent::Received { session, message } => self.received(session, message),
            TxRelayEvent::Stepped {
                session,
                effects,
                state,
                ..
            } => {
                for effect in effects {
                    self.effect(session, effect);
                }
                self.state(session, state);
            }
            TxRelayEvent::InvSent { session, entries } => self.inv_sent(session, &entries),
            TxRelayEvent::Ended { .. } => {}
        }
    }

    /// The session `session`, which the hook opened before any other event
    /// of it.
    fn session(&mut self, session: u64) -> &mut Session {
        self.sessions.entry(session).or_default()
    }

    /// A message from the peer reached `session`'s routine.
    fn received(&mut self, session: u64, message: Received) {
        let state = self.session(session);
        let cooling = state.state.cooling;
        match message {
            Received::Hello { .. } => state.hello = true,
            Received::Inv(entries) => {
                let early = !state.hello;
                for (id, size) in entries {
                    state.sizes.entry(TxKey(id)).or_default().insert(size);
                    state.tell(TxKey(id));
                }
                if early {
                    // The node holds what it heard until the peer's Hello.
                    self.trigger(HELLO_FIRST);
                }
            }
            Received::GetTxs(id) => {
                let probe = !state.sent.contains(&TxKey(id)) && self.admitted.contains(&TxKey(id));
                if probe {
                    self.trigger(NO_PROBE);
                }
            }
            Received::Tx { id, size } => {
                let asked = state.asked.contains(&TxKey(id));
                let sized = state
                    .sizes
                    .get(&TxKey(id))
                    .is_some_and(|sizes| sizes.contains(&size));
                if !asked || !sized {
                    self.trigger(REQUESTED_BODIES_ONLY);
                }
                if cooling {
                    self.trigger(COOLING_BODIES_DROPPED);
                }
            }
            Received::NotFound(id) => {
                // The peer says it lacks the id, so the node may tell it.
                state.told.remove(&TxKey(id));
            }
        }
    }

    /// One effect of a step on `session`.
    fn effect(&mut self, session: u64, effect: SimEffect) {
        match effect {
            SimEffect::Ask(id) => {
                let state = self.session(session);
                let early = !state.hello;
                state.asked.insert(TxKey(id));
                self.apply(HELLO_FIRST);
                if early {
                    self.violate(
                        HELLO_FIRST,
                        "the node asks for nothing before the peer's Hello",
                        format!("session {session} asked for {:?} first", TxKey(id)),
                    );
                }
            }
            SimEffect::Serve(id) => {
                let sent = self.session(session).sent.contains(&TxKey(id));
                self.apply(NO_PROBE);
                if !sent {
                    self.violate(
                        NO_PROBE,
                        "the node serves only ids it announced on the session",
                        format!(
                            "session {session} served {:?}, never announced on it",
                            TxKey(id)
                        ),
                    );
                }
            }
            SimEffect::NotFound(_) => {}
            SimEffect::Submit {
                requested,
                body,
                size,
                cooling,
            } => self.submitted(session, requested, body, size, cooling),
            SimEffect::Park => {
                let state = self.session(session);
                let (readmitted, peer) = (state.readmitted, state.peer);
                self.parked.extend(peer);
                self.apply(STALLED_PEERS_PARKED);
                if readmitted {
                    self.violate(
                        STALLED_PEERS_PARKED,
                        "a stall in the session a park readmitted closes the connection",
                        format!("session {session}, readmitted, was parked again"),
                    );
                }
            }
            SimEffect::Close { session, outcome } => self.machine_closed(session, outcome),
        }
    }

    /// A body went to the pool from `session`.
    fn submitted(
        &mut self,
        session: u64,
        requested: Option<UnminedTxId>,
        body: UnminedTxId,
        size: u32,
        cooling: bool,
    ) {
        let state = self.session(session);
        let asked = requested.is_some_and(|id| state.asked.contains(&TxKey(id)));
        let sized = requested
            .and_then(|id| state.sizes.get(&TxKey(id)))
            .is_some_and(|sizes| sizes.contains(&size));
        state.tell(TxKey(body));
        self.apply(REQUESTED_BODIES_ONLY);
        self.apply(COOLING_BODIES_DROPPED);
        let body = TxKey(body);
        if !asked || requested.map(TxKey) != Some(body) || !sized {
            self.violate(
                REQUESTED_BODIES_ONLY,
                "a body reaches the pool only for a request on its session, under the requested id, at the announced size",
                format!(
                    "session {session} submitted {body:?} of {size} bytes for request {:?}",
                    requested.map(TxKey)
                ),
            );
        }
        if cooling {
            self.violate(
                COOLING_BODIES_DROPPED,
                "a cooling peer's bodies never reach the pool",
                format!("session {session} submitted {body:?} while its peer cooled"),
            );
        }
    }

    /// The machine closes `session`'s connection with `outcome`.
    fn machine_closed(&mut self, session: u64, outcome: CloseOutcome) {
        let CloseOutcome::Neutral {
            cause: NeutralCause::StallPolicy,
            ..
        } = outcome
        else {
            return;
        };
        let state = self.session(session);
        let (readmitted, peer) = (state.readmitted, state.peer);
        let parked_before = peer.is_some_and(|peer| self.parked.contains(&peer));
        self.apply(STALLED_PEERS_PARKED);
        if readmitted && parked_before {
            self.trigger(STALLED_PEERS_PARKED);
        } else {
            self.violate(
                STALLED_PEERS_PARKED,
                "only a stall in the session a park readmitted closes the connection",
                format!("session {session} closed with {outcome} on its peer's first stall"),
            );
        }
    }

    /// `session` holds `state` after a step.
    fn state(&mut self, session: u64, state: SimState) {
        self.session(session).state = state;
        let mut checks = Vec::new();
        let requests = state.in_flight + state.unsettled;
        checks.push((
            REQUESTS_WITHIN_LIMIT,
            requests >= MAX_REQUESTS_PER_PEER,
            requests > MAX_REQUESTS_PER_PEER
                || state
                    .hello_serves
                    .is_some_and(|serves| state.asking > serves),
            format!(
                "{} in flight and {} unsettled, {} asking under a Hello of {:?}",
                state.in_flight, state.unsettled, state.asking, state.hello_serves
            ),
        ));
        checks.push((
            SERVING_WITHIN_LIMIT,
            state.serving >= MAX_SERVES,
            state.serving > MAX_SERVES,
            format!("{} requests held unanswered", state.serving),
        ));
        let held = state.pending.max(state.waiting);
        checks.push((
            PENDING_WITHIN_BOUND,
            held >= PENDING_PER_PEER,
            held > PENDING_PER_PEER,
            format!("{} pending and {} waiting", state.pending, state.waiting),
        ));
        checks.push((
            SKIP_SET_WITHIN_BOUND,
            state.known >= KNOWN_CAP,
            state.known > KNOWN_CAP,
            format!("{} ids in the skip set", state.known),
        ));
        for (id, at_bound, past, what) in checks {
            if past {
                self.violate(
                    id,
                    "within production's bound",
                    format!("session {session}: {what}"),
                );
            } else if at_bound {
                self.trigger(id);
            } else {
                self.apply(id);
            }
        }
    }

    /// The node announced `entries` on `session`.
    fn inv_sent(&mut self, session: u64, entries: &[(UnminedTxId, u32)]) {
        self.apply(ANNOUNCE_AFTER_ADMISSION);
        for (id, _) in entries {
            let id = TxKey(*id);
            if !self.admitted.contains(&id) {
                self.violate(
                    ANNOUNCE_AFTER_ADMISSION,
                    "the node announces only ids its pool admitted",
                    format!("session {session} announced {id:?}, which the pool never admitted"),
                );
            }
            let state = self.session(session);
            state.sent.insert(id);
            if state.told(id) {
                self.violate(
                    ANNOUNCE_AFTER_ADMISSION,
                    "the node never announces an id on a session that announced or supplied it",
                    format!("session {session} announced {id:?} back to its peer"),
                );
            }
            // Another session's peer told the node about it, and this one
            // still hears of it.
            if self
                .sessions
                .iter()
                .any(|(other, state)| *other != session && state.told(id))
            {
                self.trigger(ANNOUNCE_AFTER_ADMISSION);
            }
        }
    }

    /// Reads the log entries since the last read: each connection's peer,
    /// and each close the node decided.
    pub fn read_log(&mut self, log: &ObservationLog) {
        let entries = log.entries();
        let start = self.log_read.min(entries.len());
        let mut closes = Vec::new();
        for entry in &entries[start..] {
            match &entry.obs {
                Observation::Transport {
                    connection: Some(connection),
                    remote,
                    event: TransportFact::Registered,
                    ..
                } => {
                    if let Some(peer) = remote.parse::<SocketAddr>().ok().and_then(peer_of) {
                        self.conn_peers.insert(*connection, peer);
                    }
                }
                Observation::ConnectionClosed { connection, cause } => {
                    closes.push((*connection, *cause));
                }
                _ => {}
            }
        }
        self.log_read = entries.len();
        for (connection, cause) in closes {
            self.closed(connection, cause);
        }
    }

    /// The node closed `connection` with `cause`.
    fn closed(&mut self, connection: u64, cause: CloseOutcome) {
        let Some(row) = cause
            .row()
            .filter(|row| row.id().starts_with("relay-close-"))
        else {
            return;
        };
        let peer = self.conn_peers.get(&connection).copied();
        let broke = peer
            .and_then(|peer| self.broken.get(&peer))
            .is_some_and(|rows| rows.contains(row.id()));
        if broke {
            self.trigger(FAULT_CLOSE_NAMES_A_BROKEN_ROW);
        } else {
            self.violate(
                FAULT_CLOSE_NAMES_A_BROKEN_ROW,
                "a tx-relay fault close names a row the peer broke on that connection",
                format!(
                    "connection {connection} of peer {peer:?} closed with {row}, and the peer broke only {:?}",
                    peer.and_then(|peer| self.broken.get(&peer))
                ),
            );
        }
    }
}

/// The view a `tx-relay` check reads.
pub type RelayView = SharedRelay;

/// Each property's trigger history, in [`IDS`] order.
const TRIGGERS: [fn(&RelayView, &ObservationLog) -> bool; 11] = [
    triggered::<0>,
    triggered::<1>,
    triggered::<2>,
    triggered::<3>,
    triggered::<4>,
    triggered::<5>,
    triggered::<6>,
    triggered::<7>,
    triggered::<8>,
    triggered::<9>,
    triggered::<10>,
];

/// Whether the run reached the case `IDS[I]`'s witness needs. A trigger
/// history is a plain function, so each ID takes its own instance.
fn triggered<const I: usize>(view: &RelayView, log: &ObservationLog) -> bool {
    let mut evidence = lock(view);
    evidence.read_log(log);
    evidence
        .facts
        .get(IDS[I])
        .is_some_and(|fact| fact.triggered)
}

/// Every property, in [`IDS`] order.
pub fn properties<S: HasView<View = RelayView>>() -> Vec<Property<S>> {
    IDS.iter()
        .zip(CLAIMS)
        .zip(TRIGGERS)
        .map(|((id, claim), trigger)| Property {
            id: PropertyId::new(id),
            claim,
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::View, Source::Closes],
            status: Status::Active,
            check: check_fact::<S>,
            witness: Some(TriggerHistory::View(trigger)),
        })
        .collect()
}

/// Judges the property the check's ID names from its [`Fact`], after the
/// log's new closes are read.
fn check_fact<S: HasView<View = RelayView>>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let id = IDS
        .iter()
        .copied()
        .find(|id| *id == cx.id().as_str())
        .expect("only this module's IDs use this check");
    let view = cx.view();
    let mut evidence = lock(&view);
    evidence.read_log(cx.log);
    let fact = evidence.facts.get(id).cloned().unwrap_or_default();
    drop(evidence);
    if let Some((expected, actual)) = fact.violation {
        return cx.violated(expected, actual);
    }
    if fact.applied {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}
