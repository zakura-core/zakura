//! Cross-family session properties (properties.md, Cross-family).

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    time::Duration,
};

use zakura_network::zakura::{ZakuraBlockSyncConfig, ZAKURA_STREAM_BLOCK_SYNC};

use crate::{
    model::is_fault_cause,
    observe::{BlockSyncFact, Entry, SessionStepFact, TransportFact},
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
        witness: Some(TriggerHistory::View(honest_frame_judged::<S::View>)),
    }
}

/// [`HONEST_NEVER_PUNISHED`]'s trigger: on the connection of a peer the
/// scenario marks `HonestPeer`, the node judged a frame, so it had a frame
/// it could have punished that peer for, or it punished the peer, as the
/// check counts a punishment, even before judging anything. The
/// connection's peer is found as the check finds a punished one's, from its
/// latest transport row before the event.
fn honest_frame_judged<V: HonestPeers>(view: &V, log: &ObservationLog) -> bool {
    let entries = log.entries();
    entries.iter().enumerate().any(|(at, entry)| {
        let connection = match &entry.obs {
            Observation::FrameJudged { connection, .. } | Observation::Misbehavior { connection } => {
                *connection
            }
            Observation::ConnectionClosed { connection, cause } if is_fault_cause(cause) => {
                *connection
            }
            _ => return false,
        };
        remote_of(&entries[..at], connection)
            .and_then(peer_at)
            .is_some_and(|peer| view.is_honest(peer))
    })
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
/// connection's session carried, and a superseded block-sync routine's
/// generation-gated peer-registry write never lands on its replacement's
/// entry.
pub const REPLACEMENT_PRESERVES_AUTHORIZATION: &str = "session/replacement-preserves-authorization";

/// A block-sync request that waits on its peer closes no shared connection
/// and ends no other service's session on it: while the request waits
/// within its request timeout, the node decides no close with a recorded
/// cause on the request's connection, and no header-sync or discovery
/// session generation on it exits unless a close was decided first. Only
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
                    node decides no close with a recorded cause on the request's connection, and \
                    header sync and discovery keep the sessions they share it with: no session \
                    generation of theirs on it exits before a close is decided",
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
                replaced session names its replacement or another service's session; and no \
                block-sync routine's write through one of the peer registry's \
                generation-gated writers, each of which reports its attempts and landings, \
                lands on an entry another generation owns, so a superseded routine that \
                writes after its replacement's admission, matching the entry by peer ID \
                alone, changes nothing the replacement owns",
        kind: Kind::Safety,
        assumes: &[],
        // The transport tap's session rows, which only a harness on the
        // simulated network logs, and the block tap's admission, routine
        // and stale registry write rows.
        needs: &[Source::Datagrams, Source::View],
        status: Status::Active,
        check: check_replacement_preserves_authorization::<S>,
        witness: Some(TriggerHistory::Log(replaced_or_superseded_wrote)),
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

/// `obs` as a block-sync fact, if it is one.
fn block_fact(obs: &Observation) -> Option<&BlockSyncFact> {
    match obs {
        Observation::BlockSync { event, .. } => Some(event),
        _ => None,
    }
}

/// Whether `fact` shows a routine's generation-gated registry write meeting
/// an entry another generation owns: the write's attempt, which each gated
/// writer reports before its gate, or its landing.
fn stale_registry_write(fact: &BlockSyncFact) -> bool {
    matches!(fact, BlockSyncFact::StaleRegistryWrite { .. })
}

/// Checks [`REPLACEMENT_PRESERVES_AUTHORIZATION`] at each step with a
/// session row or a stale registry write. A session row applies on a
/// connection that replaces an earlier one to the same peer. A stale
/// registry write's attempt applies, and its landing is a violation. It
/// folds the log's earlier rows each time, since a run admits only a few
/// dozen sessions.
fn check_replacement_preserves_authorization<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let step = cx.step;
    let judged_row = |obs: &Observation| {
        session_row(obs).is_some() || block_fact(obs).is_some_and(stale_registry_write)
    };
    if !cx.observations().any(judged_row) {
        return Verdict::NotApplicable;
    }
    let mut sessions = Sessions::default();
    let mut judged = false;
    let mut findings = Vec::new();
    for entry in cx.log.entries() {
        if let Some(fact) = block_fact(&entry.obs) {
            if entry.step != step {
                continue;
            }
            judged |= stale_registry_write(fact);
            if let BlockSyncFact::StaleRegistryWrite {
                peer,
                writer,
                generation,
                entry_generation,
                landed: true,
            } = fact
            {
                findings.push((
                    "no generation-gated peer-registry write lands on an entry another \
                     routine generation owns"
                        .to_owned(),
                    format!(
                        "routine generation {generation}'s {writer} landed on peer {peer}'s \
                         entry, which generation {entry_generation} owns"
                    ),
                ));
            }
            continue;
        }
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

/// [`REPLACEMENT_PRESERVES_AUTHORIZATION`]'s trigger history: either half
/// of its claim met its condition. The node admitted a session on a
/// connection that replaces an earlier one to the same peer, or a
/// block-sync routine attempted a generation-gated registry write on an
/// entry another generation owns. A violation of one half can end a run
/// before the other half's condition arrives, so each suffices.
fn replaced_or_superseded_wrote(log: &ObservationLog) -> bool {
    let mut sessions = Sessions::default();
    log.entries()
        .iter()
        .any(|entry| match block_fact(&entry.obs) {
            Some(fact) => stale_registry_write(fact),
            None => session_row(&entry.obs).is_some_and(|row| sessions.fold(row).is_some()),
        })
}

/// One block-sync request's wait: the connection its routine runs on, the
/// peer it waits on, and when it was published, in nanoseconds of virtual
/// time.
struct Wait {
    connection: Option<u64>,
    peer: Option<String>,
    since_ns: u64,
}

/// The block-sync requests that wait on their peers, folded from the log.
#[derive(Default)]
struct Waits {
    /// Each routine generation's connection and peer, from its admission.
    routines: BTreeMap<u64, (u64, String)>,
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
                peer,
                connection,
                generation,
                ..
            } => {
                self.routines
                    .insert(*generation, (*connection, peer.clone()));
                return false;
            }
            BlockSyncFact::Request { owner, step, .. } if step == "published" => {
                let routine = self.routines.get(&owner.session_id);
                let wait = Wait {
                    connection: routine.map(|(connection, _)| *connection),
                    peer: routine.map(|(_, peer)| peer.clone()),
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
            // Only a body from the peer the request waits on ends its wait,
            // so a late body from another mirror leaves a stalled mirror's
            // request waiting.
            BlockSyncFact::HandoffStarted { peer, owner, .. } => {
                let key = (owner.session_id, owner.request_id);
                let from_its_peer = self
                    .waiting
                    .get(&key)
                    .is_some_and(|wait| wait.peer.as_ref() == Some(peer));
                if !from_its_peer {
                    return false;
                }
                owner
            }
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
/// wait, decides a close with a recorded cause, or exits a session
/// generation. A wait that ends holds. A close with a recorded cause on a
/// connection with a waiting request violates it, and so does a header-sync
/// or discovery generation's exit there before any close was decided. A waiting request whose routine no admission row names is
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
                event: TransportFact::CloseDecided { cause: Some(_) }
                    | TransportFact::SessionLifecycle {
                        step: SessionStepFact::GenerationExited { .. },
                        ..
                    },
                ..
            }
        )
    };
    if !cx.observations().any(judged) {
        return Verdict::NotApplicable;
    }
    let mut waits = Waits::default();
    // The node's connections a close was decided on, with or without a
    // recorded cause: their sessions end with them.
    let mut closing = BTreeSet::new();
    let mut verdict = Verdict::NotApplicable;
    let mut findings = Vec::new();
    let mut unmapped = None;
    for entry in cx.log.entries() {
        let ended = waits.fold(&entry.obs);
        if let Observation::Transport {
            instance: 0,
            connection: Some(connection),
            event: TransportFact::CloseDecided { .. },
            ..
        } = &entry.obs
        {
            closing.insert(*connection);
        }
        if entry.step != step {
            continue;
        }
        if ended {
            verdict = Verdict::Holds;
        }
        if let Observation::Transport {
            instance: 0,
            connection: Some(connection),
            event:
                TransportFact::SessionLifecycle {
                    kind,
                    step: SessionStepFact::GenerationExited { generation, .. },
                },
            ..
        } = &entry.obs
        {
            let waiting = waits.on(*connection);
            if *kind != ZAKURA_STREAM_BLOCK_SYNC
                && !closing.contains(connection)
                && !waiting.is_empty()
            {
                findings.push((
                    "header sync and discovery keep their sessions on a connection while a \
                     block-sync request on it waits"
                        .to_owned(),
                    format!(
                        "kind {kind}'s session generation {generation} on connection \
                         {connection} exited while requests {} waited on it",
                        waiting.join(", ")
                    ),
                ));
            }
            continue;
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
