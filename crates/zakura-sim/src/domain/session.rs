//! Cross-family session properties (properties.md, Cross-family).

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
};

use crate::{
    model::is_fault_cause,
    observe::Entry,
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

/// The session properties a wire harness with several services on one
/// connection reaches: [`REPLACEMENT_PRESERVES_AUTHORIZATION`].
pub fn wire_properties<S: HasView>() -> Vec<Property<S>> {
    vec![Property {
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
    }]
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
