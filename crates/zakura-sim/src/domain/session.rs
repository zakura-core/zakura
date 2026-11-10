//! Cross-family session properties (properties.md, Cross-family).

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    time::Duration,
};

use zakura_network::zakura::ZakuraBlockSyncConfig;

use crate::{
    model::is_fault_cause,
    observe::{BlockSyncFact, Entry, TransportFact},
    peer::peer_of,
    property::{
        Assume, CheckCtx, HasView, Kind, Property, PropertyId, Source, Status, TriggerHistory,
        Verdict,
    },
    Observation, ObservationLog,
};

/// A peer the scenario marks `HonestPeer` is never punished.
pub const HONEST_NEVER_PUNISHED: &str = "session/honest-never-punished";

/// What [`HONEST_NEVER_PUNISHED`] reads from a harness's view: the peers
/// the scenario marks `HonestPeer`.
pub trait HonestPeers {
    /// True if the scenario marks peer `index`, named `peer-<index>`,
    /// `HonestPeer`.
    fn is_honest(&self, index: usize) -> bool;
}

/// [`HONEST_NEVER_PUNISHED`]: the node records no fault close cause on the
/// connection of a peer the scenario marks `HonestPeer`, and asks storage
/// for no `Misbehavior` against it. An honest peer follows every row on
/// every stream, so no fault, however labelled, justifies its punishment.
///
/// It reads the node's first recorded close cause, not the wire close
/// code, which waits for P3c. Read timeouts and transport errors have
/// neutral causes (P3e), so lossy runs still hold. A connection's peer is
/// the one [`peer_of`] finds behind the remote address its transport rows
/// name. A scenario that marks no peer `HonestPeer` doesn't declare the
/// assumption, so the property is `NotApplicable` there.
pub fn honest_never_punished<S>() -> Property<S>
where
    S: HasView,
    S::View: HonestPeers,
{
    Property {
        id: PropertyId::new(HONEST_NEVER_PUNISHED),
        claim: "a peer marked HonestPeer is never punished",
        kind: Kind::Safety,
        assumes: &[Assume::HonestPeer],
        needs: &[Source::View, Source::Closes, Source::Storage],
        status: Status::Active,
        check: check_honest_never_punished::<S>,
        witness: None,
    }
}

/// Checks [`HONEST_NEVER_PUNISHED`] on the step's observations. It applies
/// at each step with a transport row of an honest peer's connection. Each
/// fault close or `Misbehavior` of an honest peer's connection is a
/// violation of its own. A punished connection that no earlier transport
/// row names is missing evidence.
fn check_honest_never_punished<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: HonestPeers,
{
    let view = cx.view();
    let entries = cx.log.entries();
    let start = entries.partition_point(|e| e.step < cx.step);
    let mut verdict = Verdict::NotApplicable;
    let mut punished = Vec::new();
    for (at, entry) in entries.iter().enumerate().skip(start) {
        match &entry.obs {
            Observation::Transport { remote, .. }
                if peer_at(remote).is_some_and(|peer| view.is_honest(peer)) =>
            {
                verdict = Verdict::Holds;
            }
            Observation::ConnectionClosed { connection, cause } if is_fault_cause(cause) => {
                punished.push((at, *connection, format!("a close with cause {cause}")));
            }
            Observation::Misbehavior { connection } => {
                punished.push((at, *connection, "a Misbehavior".to_owned()));
            }
            _ => {}
        }
    }
    let mut violations = Vec::new();
    for (at, connection, punishment) in punished {
        let Some(remote) = remote_of(&entries[..at], connection) else {
            cx.missing_evidence(format!(
                "connection {connection} was punished, but no transport row names its remote"
            ));
            return Verdict::NotApplicable;
        };
        if let Some(peer) = peer_at(remote).filter(|peer| view.is_honest(*peer)) {
            violations.push((
                format!("honest peer-{peer}'s connection {connection} is never punished"),
                punishment,
            ));
        }
    }
    let mut violations = violations.into_iter();
    let Some((expected, actual)) = violations.next() else {
        return verdict;
    };
    for (expected, actual) in violations {
        cx.record_violation(expected, actual);
    }
    cx.violated(expected, actual)
}

/// The scenario peer that binds `remote`, if any.
fn peer_at(remote: &str) -> Option<usize> {
    remote.parse::<SocketAddr>().ok().and_then(peer_of)
}

/// The remote address of `connection`, from its latest transport row in
/// `entries`.
fn remote_of(entries: &[Entry], connection: u64) -> Option<&str> {
    entries.iter().rev().find_map(|entry| match &entry.obs {
        Observation::Transport {
            connection: Some(c),
            remote,
            ..
        } if *c == connection => Some(remote.as_str()),
        _ => None,
    })
}

/// A service session the node admits on a connection that replaces an
/// earlier connection to the same peer carries a session ID no earlier
/// connection's session carried.
pub const REPLACEMENT_PRESERVES_AUTHORIZATION: &str = "session/replacement-preserves-authorization";

/// A block-sync request that waits on its peer closes no shared connection:
/// the node decides no close with a recorded cause on the request's
/// connection while the request waits within its request timeout. Only
/// the request's first body, a reset, a skipped or failed write, or the
/// timeout ends the wait: a request whose routine drops it unanswered still
/// waits, since its peer may still answer.
pub const STALLED_SERVICE_ISOLATED: &str = "session/stalled-service-isolated";

/// How long a block-sync request must wait, in virtual time, before
/// [`STALLED_SERVICE_ISOLATED`]'s trigger history counts it as a stall:
/// several round trips of the simulated network, and less than any saved
/// stall.
const STALL_WITNESS: Duration = Duration::from_millis(500);

/// The session properties a wire harness with several services on one
/// connection reaches: [`REPLACEMENT_PRESERVES_AUTHORIZATION`] and
/// [`STALLED_SERVICE_ISOLATED`].
pub fn wire_properties<S: HasView>() -> Vec<Property<S>> {
    vec![
        replacement_preserves_authorization(),
        Property {
            id: PropertyId::new(STALLED_SERVICE_ISOLATED),
            claim: "while a block-sync request waits on its peer, from its publication until its \
                    first body arrives, block sync resets it, or its request timeout passes, the \
                    node decides no close with a recorded cause on the request's connection, so \
                    header sync and discovery keep the sessions they share it with",
            kind: Kind::Safety,
            // A wire harness's mirrors all serve honestly, so no recorded
            // cause justifies a close while one of their requests waits.
            assumes: &[],
            // The block tap's request rows and the transport tap's close
            // rows.
            needs: &[Source::View, Source::Datagrams],
            status: Status::Active,
            check: check_stalled_service_isolated::<S>,
            witness: Some(TriggerHistory::Log(a_request_stalled)),
        },
    ]
}

/// [`REPLACEMENT_PRESERVES_AUTHORIZATION`].
fn replacement_preserves_authorization<S: HasView>() -> Property<S> {
    Property {
        id: PropertyId::new(REPLACEMENT_PRESERVES_AUTHORIZATION),
        claim: "every service session the node admits on a connection that replaces an \
                earlier connection to the same peer carries a session ID that no session on \
                an earlier connection carried, so no frame, credit or completion keyed by a \
                replaced session names its replacement or another service's session",
        kind: Kind::Safety,
        assumes: &[],
        // The transport tap's session rows, which only a harness on the
        // simulated network logs.
        needs: &[Source::Datagrams],
        status: Status::Active,
        check: check_replacement_preserves_authorization::<S>,
        witness: Some(TriggerHistory::Log(replacement_admitted)),
    }
}

/// One session row: its connection, its peer and its session ID.
type SessionRow<'a> = (u64, &'a str, u64);

/// `obs` as a session row, if it is one.
fn session_row(obs: &Observation) -> Option<SessionRow<'_>> {
    match obs {
        Observation::Session {
            connection,
            peer,
            session_id,
            ..
        } => Some((*connection, peer.as_str(), *session_id)),
        _ => None,
    }
}

/// Each peer's connections, in the order their first session arrived, and
/// each connection's session IDs.
#[derive(Default)]
struct Sessions {
    connections: BTreeMap<String, Vec<u64>>,
    ids: BTreeMap<u64, BTreeSet<u64>>,
}

impl Sessions {
    /// Folds `row`. Returns the session IDs of every earlier connection to
    /// the row's peer if the row's connection replaces one, or `None`.
    fn fold(&mut self, (connection, peer, session_id): SessionRow<'_>) -> Option<BTreeSet<u64>> {
        let connections = self.connections.entry(peer.to_owned()).or_default();
        if !connections.contains(&connection) {
            connections.push(connection);
        }
        let earlier: BTreeSet<u64> = connections
            .iter()
            .take_while(|earlier| **earlier != connection)
            .flat_map(|earlier| self.ids.get(earlier).into_iter().flatten().copied())
            .collect();
        let replaces = connections.first() != Some(&connection);
        self.ids.entry(connection).or_default().insert(session_id);
        replaces.then_some(earlier)
    }
}

/// Checks [`REPLACEMENT_PRESERVES_AUTHORIZATION`] at each step with a
/// session row. It applies to a row on a connection that replaces an
/// earlier one to the same peer. It folds the log's earlier rows each time,
/// since a run admits only a few dozen sessions.
fn check_replacement_preserves_authorization<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let step = cx.step;
    if cx.observations().all(|obs| session_row(obs).is_none()) {
        return Verdict::NotApplicable;
    }
    let mut sessions = Sessions::default();
    let mut judged = false;
    let mut findings = Vec::new();
    for entry in cx.log.entries() {
        let Some(row) = session_row(&entry.obs) else {
            continue;
        };
        let earlier = sessions.fold(row);
        if entry.step != step {
            continue;
        }
        let Some(earlier) = earlier else {
            continue;
        };
        judged = true;
        let (connection, peer, session_id) = row;
        if earlier.contains(&session_id) {
            findings.push((
                "a replacement connection's session carries an ID no earlier connection's \
                 session carried"
                    .to_owned(),
                format!(
                    "connection {connection} to {peer} admitted session {session_id}, which an \
                     earlier connection to {peer} carried"
                ),
            ));
        }
    }
    let mut findings = findings.into_iter();
    match findings.next() {
        Some((expected, actual)) => {
            for (expected, actual) in findings {
                cx.record_violation(expected, actual);
            }
            cx.violated(expected, actual)
        }
        None if judged => Verdict::Holds,
        None => Verdict::NotApplicable,
    }
}

/// [`REPLACEMENT_PRESERVES_AUTHORIZATION`]'s trigger history: the node
/// admitted a session on a connection that replaces an earlier one to the
/// same peer.
fn replacement_admitted(log: &ObservationLog) -> bool {
    let mut sessions = Sessions::default();
    log.entries()
        .iter()
        .filter_map(|entry| session_row(&entry.obs))
        .any(|row| sessions.fold(row).is_some())
}

/// One block-sync request's wait: the connection its routine runs on and
/// when it was published, in nanoseconds of virtual time.
struct Wait {
    connection: Option<u64>,
    since_ns: u64,
}

/// The block-sync requests that wait on their peers, folded from the log.
#[derive(Default)]
struct Waits {
    /// Each routine generation's connection, from its admission.
    connections: BTreeMap<u64, u64>,
    /// Each waiting request, by its owner's generation and request ID.
    waiting: BTreeMap<(u64, u64), Wait>,
    /// The virtual time of the last `TimeAdvanced` row.
    now_ns: u64,
    /// The longest wait that ended, in nanoseconds.
    longest_ns: u64,
}

impl Waits {
    /// Folds `obs`. Returns true if it ended a wait.
    fn fold(&mut self, obs: &Observation) -> bool {
        let fact = match obs {
            Observation::TimeAdvanced { to_ns, .. } => {
                self.now_ns = *to_ns;
                return false;
            }
            Observation::BlockSync { event, .. } => event,
            _ => return false,
        };
        let ended = match fact {
            BlockSyncFact::SessionAdmitted {
                connection,
                generation,
                ..
            } => {
                self.connections.insert(*generation, *connection);
                return false;
            }
            BlockSyncFact::Request { owner, step, .. } if step == "published" => {
                let wait = Wait {
                    connection: self.connections.get(&owner.session_id).copied(),
                    since_ns: self.now_ns,
                };
                self.waiting
                    .insert((owner.session_id, owner.request_id), wait);
                return false;
            }
            BlockSyncFact::Request { owner, step, .. }
                if matches!(step.as_str(), "reset" | "expired" | "delivery-failed") =>
            {
                owner
            }
            BlockSyncFact::HandoffStarted { owner, .. } => owner,
            _ => return false,
        };
        let Some(wait) = self.waiting.remove(&(ended.session_id, ended.request_id)) else {
            return false;
        };
        self.longest_ns = self
            .longest_ns
            .max(self.now_ns.saturating_sub(wait.since_ns));
        true
    }

    /// The requests on `connection` that wait within the request timeout of
    /// block sync's default config, which wire harnesses run, as
    /// `generation/request` labels.
    fn on(&self, connection: u64) -> Vec<String> {
        let timeout = u64::try_from(ZakuraBlockSyncConfig::default().request_timeout.as_nanos())
            .unwrap_or(u64::MAX);
        self.waiting
            .iter()
            .filter(|(_, wait)| {
                wait.connection == Some(connection)
                    && self.now_ns.saturating_sub(wait.since_ns) < timeout
            })
            .map(|((generation, request), _)| format!("{generation}/{request}"))
            .collect()
    }
}

/// Checks [`STALLED_SERVICE_ISOLATED`] at each step that ends a request's
/// wait or decides a close with a recorded cause. A wait that ends holds.
/// A close with a recorded cause on a connection with a waiting request
/// violates it. A waiting request whose routine no admission row names is
/// missing evidence. It folds the log's earlier rows each time, since a run
/// publishes only a few dozen requests.
fn check_stalled_service_isolated<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let step = cx.step;
    let judged = |obs: &Observation| {
        matches!(
            obs,
            Observation::BlockSync {
                event: BlockSyncFact::HandoffStarted { .. } | BlockSyncFact::Request { .. },
                ..
            } | Observation::Transport {
                event: TransportFact::CloseDecided { cause: Some(_) },
                ..
            }
        )
    };
    if !cx.observations().any(judged) {
        return Verdict::NotApplicable;
    }
    let mut waits = Waits::default();
    let mut verdict = Verdict::NotApplicable;
    let mut findings = Vec::new();
    let mut unmapped = None;
    for entry in cx.log.entries() {
        let ended = waits.fold(&entry.obs);
        if entry.step != step {
            continue;
        }
        if ended {
            verdict = Verdict::Holds;
        }
        let Observation::Transport {
            connection: Some(connection),
            event: TransportFact::CloseDecided { cause: Some(cause) },
            ..
        } = &entry.obs
        else {
            continue;
        };
        if let Some(((generation, request), _)) = waits
            .waiting
            .iter()
            .find(|(_, wait)| wait.connection.is_none())
        {
            unmapped = Some(format!(
                "request {request} of routine generation {generation} waits, but no \
                 block-sync admission row names its connection"
            ));
        }
        let waiting = waits.on(*connection);
        if !waiting.is_empty() {
            findings.push((
                "the node decides no close with a recorded cause on a connection while a \
                 block-sync request on it waits"
                    .to_owned(),
                format!(
                    "the node closed connection {connection} with cause {cause} while \
                     requests {} waited on it",
                    waiting.join(", ")
                ),
            ));
        }
    }
    if let Some(detail) = unmapped {
        cx.missing_evidence(detail);
        return Verdict::NotApplicable;
    }
    let mut findings = findings.into_iter();
    match findings.next() {
        Some((expected, actual)) => {
            for (expected, actual) in findings {
                cx.record_violation(expected, actual);
            }
            cx.violated(expected, actual)
        }
        None => verdict,
    }
}

/// [`STALLED_SERVICE_ISOLATED`]'s trigger history: a block-sync request
/// waited at least [`STALL_WITNESS`] before its first body arrived, block
/// sync reset it, or the log ended.
fn a_request_stalled(log: &ObservationLog) -> bool {
    let mut waits = Waits::default();
    for entry in log.entries() {
        waits.fold(&entry.obs);
    }
    let open = waits
        .waiting
        .values()
        .map(|wait| waits.now_ns.saturating_sub(wait.since_ns))
        .max()
        .unwrap_or(0);
    let threshold = u64::try_from(STALL_WITNESS.as_nanos()).expect("half a second fits u64");
    waits.longest_ns.max(open) >= threshold
}
