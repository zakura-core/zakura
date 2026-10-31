//! Cross-family session properties (properties.md, Cross-family).

use std::net::SocketAddr;

use crate::{
    model::is_fault_cause,
    observe::Entry,
    peer::peer_of,
    property::{Assume, CheckCtx, HasView, Kind, Property, PropertyId, Source, Status, Verdict},
    Observation,
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
