//! The node under test's session view, beside the harness's own links.
//!
//! `header-sync/storage` runs no transport. [`Topology::link`] hands each
//! side an in-memory stream and uses its own counter as the connection, the
//! session ID and the generation. So the harness records its side as the
//! expectation: [`SessionFact::LinkOpened`] when it makes a link, and
//! [`SessionFact::LinkClosed`] when it closes one. The reactor tap records the
//! reactor's side: [`SessionFact::SessionAdmitted`] and
//! [`SessionFact::SessionEnded`]. A link a service closes itself, such as a
//! replacement retiring an unfinished exchange, is recorded at the next
//! observation, when the harness's closure count sees it. A
//! [`SessionEvidence`] keeps both sides in the order they happened and checks
//! two rules as facts arrive.
//!
//! - **Join.** Once the links report here ([`SessionEvidence::expect_links`]),
//!   every admission matches exactly one opened link by its origin's
//!   connection, and carries that link's whole origin. It must agree with
//!   that link on peer, direction and session ID, so a wrong session ID fails
//!   even when the origin is right. A link is admitted at most once. A refused link has no
//!   admission, so the reverse isn't required.
//! - **Departed sessions stay inert.** A session leaves the reactor's view when
//!   another replaces it or when its disconnect ends it. After that, no
//!   `Started` and no ordinary `Dispatched { Apply }` names it. A late
//!   completion of a call it already started is still legal. So is one apply
//!   of a selected auxiliary repair that was preparing when the session left,
//!   which the reactor keeps until its held operation ends. A repair that was
//!   already applying, or that retired, may not apply again.
//!
//! `header-sync/wire` runs the real transport, so there the view joins the
//! reactor's sessions to own-9's facts instead
//! ([`SessionEvidence::expect_transport`]). The transport tap feeds every
//! transport event as the transport reports it, so each admission is matched
//! against own-9's state at that moment, never against rows that came later.
//!
//! - **Own-9 join.** Every admission carries the origin its handler set. Exactly
//!   one header `Session` row has that origin's connection and QUIC stream, and
//!   it names the same peer and session ID. Exactly one `generation-installed`
//!   on that connection has the origin's generation. The handler installs each
//!   generation under its session ID, so the generation also equals the
//!   admitted session ID.
//! - **Own-9 ends.** Each session that leaves the reactor's view, by its end or
//!   its replacement, is retired by own-9 by the run's end, in either order:
//!   its generation's `generation-exited`, or its connection's `torn-down`. A
//!   run that ends first is inconclusive ([`SessionEvidence::end_check`]), not
//!   failed, so that rule never breaks the run.
//!
//! The view freezes with the run's log, before the scheduler's shutdown
//! resumes suspended tasks, so their cleanup can't change what the run ended
//! with. The facts stay out of the observation log, so pinned Plans keep
//! their format. A test reads them from the run's
//! [`Ports`](crate::harness::header_sync::Ports) or
//! [`WirePorts`](crate::harness::header_sync::WirePorts) after the run.
//!
//! [`Topology::link`]: super::Topology::link

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use zakura_header_chain::{BodyWorkOwner, HeaderSyncWorkOwner};
use zakura_network::zakura::{
    observe::{PortCall, ReactorEvent, RequestRef},
    ServicePeerDirection, SessionOrigin, SessionStep, TransportEvent, ZakuraPeerId,
    ZAKURA_STREAM_HEADER_SYNC,
};
use zakura_rt::sim::RunId;

use crate::peer::peer_of;

/// One session or link fact about the node under test, in run order. Peers
/// are named as the scenario names them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionFact {
    /// The harness linked the node to `peer`.
    LinkOpened {
        /// The node at the other end.
        peer: String,
        /// The link's connection ID.
        connection: u64,
        /// The session ID both sides were handed.
        session_id: u64,
        /// The node that dialed.
        dialer: String,
    },
    /// The link on `connection` closed, the first time it did: when the
    /// harness closed it, or at the next observation after a service
    /// cancelled its connection.
    LinkClosed {
        /// The node at the other end.
        peer: String,
        /// The link's connection ID.
        connection: u64,
    },
    /// The node's reactor admitted a session, as
    /// [`ReactorEvent::SessionAdmitted`] reports it.
    SessionAdmitted {
        /// The peer, or its digest if no link named it.
        peer: String,
        /// The session ID the reactor holds.
        session_id: u64,
        /// The direction the reactor holds.
        direction: ServicePeerDirection,
        /// The session it replaced.
        replaced: Option<u64>,
        /// Where the transport got it.
        origin: Option<SessionOrigin>,
    },
    /// The node's reactor ended a session on its disconnect, as
    /// [`ReactorEvent::SessionEnded`] reports it.
    SessionEnded {
        /// The peer, or its digest if no link named it.
        peer: String,
        /// The ended session.
        session_id: u64,
        /// Why its service reported the disconnect.
        reason: String,
    },
    /// Own-9 retired a session that left the reactor's view, the first time
    /// the view saw it do so.
    SessionRetired {
        /// The peer.
        peer: String,
        /// The retired session.
        session_id: u64,
        /// What own-9 reported.
        by: Retirement,
    },
}

/// How own-9 retired a session, in [`SessionFact::SessionRetired`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retirement {
    /// Its generation's `generation-exited`.
    GenerationExited,
    /// Its connection's `torn-down`.
    TornDown,
}

/// Whether own-9 retired every session that left the reactor's view, by
/// [`SessionEvidence::end_check`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EndCheck {
    /// Every departed session was retired. A view with no own-9 facts has
    /// nothing to retire.
    Complete,
    /// The run ended before own-9 retired these sessions, each named by
    /// peer and session ID.
    Inconclusive(Vec<(String, u64)>),
}

/// A run's session view, shared by the node's tap and the topology's links.
/// Cloning shares it.
#[derive(Clone, Default)]
pub struct SessionEvidence(Arc<Mutex<SessionLedger>>);

impl std::fmt::Debug for SessionEvidence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SessionEvidence")
    }
}

/// What [`SessionEvidence`] holds.
#[derive(Default)]
struct SessionLedger {
    /// The run that fills it, once a tap is bound.
    run: Option<RunId>,
    /// Whether the topology reports its links, which turns the join on.
    joined: bool,
    facts: Vec<SessionFact>,
    /// Each link by connection.
    links: HashMap<u64, Link>,
    /// Each session that left the reactor's view, by peer and session ID.
    departed: HashSet<(ZakuraPeerId, u64)>,
    /// Each live auxiliary repair with a dispatched call: its peer, and
    /// whether that call is its apply.
    repairs: HashMap<BodyWorkOwner, (ZakuraPeerId, bool)>,
    /// The repairs that were preparing when their session left, each allowed
    /// one apply.
    kept: HashSet<BodyWorkOwner>,
    /// Own-9's facts, once the transport reports here.
    own9: Option<Own9>,
    /// Set once the run's log froze. Nothing changes the view after that.
    frozen: bool,
    /// The first rule the run broke.
    broken: Option<String>,
}

/// Own-9's facts about the node's sessions, as its transport reported them,
/// and the reactor sessions they retire.
struct Own9 {
    /// Each scripted peer's ID and name, by index.
    names: Vec<(ZakuraPeerId, String)>,
    /// Each registered connection's peer, if a scripted peer binds its
    /// address.
    peers: HashMap<u64, Option<String>>,
    /// Each `Session` row so far.
    rows: Vec<Row>,
    /// Each header generation installed so far, by connection and
    /// generation.
    installed: Vec<(u64, u64)>,
    /// Each header generation whose workers finished.
    exited: HashSet<(u64, u64)>,
    /// Each torn-down connection.
    torn_down: HashSet<u64>,
    /// Each joined session's origin, by peer and session ID.
    origins: HashMap<(ZakuraPeerId, u64), SessionOrigin>,
    /// Each departed session own-9 hasn't retired yet: its peer's name, its
    /// session ID and its origin.
    open: Vec<(String, u64, SessionOrigin)>,
}

/// One own-9 `Session` row: an admitted service session.
struct Row {
    connection: u64,
    quic_stream: u64,
    kind: u16,
    session_id: u64,
    /// The peer that binds its connection's address.
    peer: Option<String>,
}

/// One link the harness made.
struct Link {
    peer: ZakuraPeerId,
    name: String,
    /// The origin both sides' peers carry.
    origin: SessionOrigin,
    session_id: u64,
    /// The node under test's side of it.
    direction: ServicePeerDirection,
    admitted: bool,
    closed: bool,
}

impl SessionEvidence {
    /// Locks the ledger. Every change completes under the lock, so a panic
    /// elsewhere can't leave it half changed.
    fn ledger(&self) -> MutexGuard<'_, SessionLedger> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Binds this view to `run`, the run of the tap that fills it.
    ///
    /// # Panics
    ///
    /// If a tap already bound it, since one view serves one run.
    pub(crate) fn bind(&self, run: RunId) {
        let mut ledger = self.ledger();
        assert!(
            ledger.run.is_none(),
            "a session view serves one run, so each run prepares its own"
        );
        ledger.run = Some(run);
    }

    /// Freezes the view with its run's log, so the events the run's
    /// shutdown cleanup reports, such as a generation's exit, change none of
    /// its facts, rules or end check.
    pub(crate) fn freeze(&self) {
        self.ledger().frozen = true;
    }

    /// Locks the ledger for a change, or `None` once it's frozen.
    fn live(&self) -> Option<MutexGuard<'_, SessionLedger>> {
        Some(self.ledger()).filter(|ledger| !ledger.frozen)
    }

    /// Turns the join on: the topology reports every link it makes here.
    pub(crate) fn expect_links(&self) {
        self.ledger().joined = true;
    }

    /// Turns the own-9 join on: the transport reports every event here.
    /// `names` gives each scripted peer's ID and name, by index, which also
    /// names the peer at a connection's address.
    pub(crate) fn expect_transport(&self, names: Vec<(ZakuraPeerId, String)>) {
        self.ledger().own9 = Some(Own9 {
            names,
            peers: HashMap::new(),
            rows: Vec::new(),
            installed: Vec::new(),
            exited: HashSet::new(),
            torn_down: HashSet::new(),
            origins: HashMap::new(),
            open: Vec::new(),
        });
    }

    /// Feeds one of the transport's events, as it reports it. Events the
    /// own-9 rules don't read, every event before
    /// [`SessionEvidence::expect_transport`], and every event once the view
    /// froze are ignored.
    pub(crate) fn transport(&self, event: &TransportEvent) {
        let Some(mut ledger) = self.live() else {
            return;
        };
        match event {
            TransportEvent::Registered { conn_id, remote } => {
                ledger.registered(*conn_id, *remote);
            }
            TransportEvent::Admitted {
                conn_id,
                quic_stream,
                kind,
                session_id,
                ..
            } => ledger.session_row(*conn_id, *quic_stream, *kind, *session_id),
            TransportEvent::SessionLifecycle {
                conn_id,
                kind: ZAKURA_STREAM_HEADER_SYNC,
                step: SessionStep::GenerationInstalled { generation, .. },
            } => ledger.generation_installed(*conn_id, *generation),
            TransportEvent::SessionLifecycle {
                conn_id,
                kind: ZAKURA_STREAM_HEADER_SYNC,
                step: SessionStep::GenerationExited { generation, .. },
            } => ledger.retire(*conn_id, Some(*generation)),
            TransportEvent::TornDown { conn_id } => ledger.retire(*conn_id, None),
            _ => {}
        }
    }

    /// Whether own-9 retired every session that left the reactor's view.
    pub fn end_check(&self) -> EndCheck {
        let ledger = self.ledger();
        let open: Vec<(String, u64)> = ledger
            .own9
            .iter()
            .flat_map(|own9| &own9.open)
            .map(|(peer, session_id, _)| (peer.clone(), *session_id))
            .collect();
        if open.is_empty() {
            EndCheck::Complete
        } else {
            EndCheck::Inconclusive(open)
        }
    }

    /// Records that the harness linked the node under test to `peer`, named
    /// `name`, with `session_id`, handing both sides `origin`. `dialer` names
    /// the node that dialed, and `direction` is the node under test's side.
    pub(crate) fn link_opened(
        &self,
        peer: &ZakuraPeerId,
        name: &str,
        origin: SessionOrigin,
        session_id: u64,
        dialer: &str,
        direction: ServicePeerDirection,
    ) {
        let connection = origin.connection;
        let Some(mut ledger) = self.live() else {
            return;
        };
        ledger.facts.push(SessionFact::LinkOpened {
            peer: name.to_owned(),
            connection,
            session_id,
            dialer: dialer.to_owned(),
        });
        let link = Link {
            peer: peer.clone(),
            name: name.to_owned(),
            origin,
            session_id,
            direction,
            admitted: false,
            closed: false,
        };
        if ledger.links.insert(connection, link).is_some() {
            ledger.break_rule(format!(
                "SessionJoin: the harness opened connection {connection} twice"
            ));
        }
    }

    /// Records that the link on `connection` closed, unless it already did.
    pub(crate) fn link_closed(&self, connection: u64) {
        let Some(mut ledger) = self.live() else {
            return;
        };
        let Some(link) = ledger.links.get_mut(&connection) else {
            return;
        };
        if std::mem::replace(&mut link.closed, true) {
            return;
        }
        let peer = link.name.clone();
        ledger
            .facts
            .push(SessionFact::LinkClosed { peer, connection });
    }

    /// Feeds one of the reactor's events, keeping the session facts and
    /// checking both rules. Events neither rule reads, and every event once
    /// the view froze, are ignored, so a tap feeds every event.
    pub(crate) fn reactor(&self, event: &ReactorEvent) {
        if let Some(mut ledger) = self.live() {
            ledger.reactor(event);
        }
    }

    /// The first rule the run broke.
    pub(crate) fn broken(&self) -> Option<String> {
        self.ledger().broken.clone()
    }

    /// Every fact so far, in order.
    pub fn facts(&self) -> Vec<SessionFact> {
        self.ledger().facts.clone()
    }
}

impl SessionLedger {
    /// Keeps `why` unless an earlier rule already broke.
    fn break_rule(&mut self, why: String) {
        self.broken.get_or_insert(why);
    }

    /// `peer`'s name from the links or the scripted peers, or its digest.
    fn name(&self, peer: &ZakuraPeerId) -> String {
        let scripted = self
            .own9
            .iter()
            .flat_map(|own9| &own9.names)
            .find(|(id, _)| id == peer)
            .map(|(_, name)| name);
        self.links
            .values()
            .find(|link| link.peer == *peer)
            .map(|link| &link.name)
            .or(scripted)
            .map_or_else(|| super::tap::peer(peer), String::clone)
    }

    /// Records that connection `conn_id` registered from `remote`, and the
    /// scripted peer that binds that address.
    fn registered(&mut self, conn_id: u64, remote: std::net::SocketAddr) {
        if let Some(own9) = &mut self.own9 {
            let peer = peer_of(remote)
                .and_then(|index| own9.names.get(index))
                .map(|(_, name)| name.clone());
            own9.peers.insert(conn_id, peer);
        }
    }

    /// Records own-9's `Session` row for a session of `kind` on
    /// `connection`'s `quic_stream`, with `session_id`.
    fn session_row(&mut self, connection: u64, quic_stream: u64, kind: u16, session_id: u64) {
        if let Some(own9) = &mut self.own9 {
            let peer = own9.peers.get(&connection).cloned().flatten();
            own9.rows.push(Row {
                connection,
                quic_stream,
                kind,
                session_id,
                peer,
            });
        }
    }

    /// Records that `connection` installed header generation `generation`.
    fn generation_installed(&mut self, connection: u64, generation: u64) {
        if let Some(own9) = &mut self.own9 {
            own9.installed.push((connection, generation));
        }
    }

    /// Records that own-9 retired `connection`'s header generation
    /// `generation`, or the whole connection when `generation` is `None`,
    /// retiring each departed session it carried.
    fn retire(&mut self, connection: u64, generation: Option<u64>) {
        let Some(own9) = &mut self.own9 else {
            return;
        };
        let by = match generation {
            Some(generation) => {
                own9.exited.insert((connection, generation));
                Retirement::GenerationExited
            }
            None => {
                own9.torn_down.insert(connection);
                Retirement::TornDown
            }
        };
        let (retired, open): (Vec<_>, Vec<_>) = std::mem::take(&mut own9.open)
            .into_iter()
            .partition(|(_, _, origin)| {
                origin.connection == connection
                    && generation.is_none_or(|generation| origin.generation == generation)
            });
        own9.open = open;
        self.facts
            .extend(
                retired
                    .into_iter()
                    .map(|(peer, session_id, _)| SessionFact::SessionRetired {
                        peer,
                        session_id,
                        by,
                    }),
            );
    }

    /// Records `peer`'s session `session_id` as gone from the reactor's view,
    /// keeping one apply for each of its repairs that was preparing. Under
    /// own-9, it's retired at once if own-9 already retired its generation
    /// or connection, and left open otherwise.
    fn depart(&mut self, peer: &ZakuraPeerId, session_id: u64) {
        if !self.departed.insert((peer.clone(), session_id)) {
            return;
        }
        let name = self.name(peer);
        if let Some(own9) = &mut self.own9 {
            if let Some(origin) = own9.origins.get(&(peer.clone(), session_id)).copied() {
                let by = if own9
                    .exited
                    .contains(&(origin.connection, origin.generation))
                {
                    Some(Retirement::GenerationExited)
                } else {
                    own9.torn_down
                        .contains(&origin.connection)
                        .then_some(Retirement::TornDown)
                };
                match by {
                    Some(by) => self.facts.push(SessionFact::SessionRetired {
                        peer: name,
                        session_id,
                        by,
                    }),
                    None => own9.open.push((name, session_id, origin)),
                }
            }
        }
        let preparing = self
            .repairs
            .iter()
            .filter(|(owner, (of, applying))| {
                owner.session_id() == session_id && of == peer && !applying
            })
            .map(|(owner, _)| *owner);
        self.kept.extend(preparing);
    }

    /// Whether `peer`'s session `session_id` left the reactor's view.
    fn departed(&self, peer: &ZakuraPeerId, session_id: u64) -> bool {
        self.departed.contains(&(peer.clone(), session_id))
    }

    /// Checks `admitted`'s session against the link its origin names.
    fn join(
        &mut self,
        peer: &ZakuraPeerId,
        session_id: u64,
        direction: ServicePeerDirection,
        origin: Option<SessionOrigin>,
    ) -> Result<(), String> {
        let name = self.name(peer);
        let Some(origin) = origin else {
            return Err(format!(
                "SessionJoin: the node admitted session {session_id} from {name} with no origin"
            ));
        };
        let Some(link) = self.links.get_mut(&origin.connection) else {
            return Err(format!(
                "SessionJoin: the node admitted session {session_id} from {name} with origin \
                 {origin:?}, which names no link"
            ));
        };
        let mismatch = if link.origin != origin {
            Some(format!("origin {:?}", link.origin))
        } else if link.peer != *peer {
            Some(format!("peer {}", link.name))
        } else if link.direction != direction {
            Some(format!("direction {:?}", link.direction))
        } else if link.session_id != session_id {
            Some(format!("session {}", link.session_id))
        } else {
            None
        };
        if let Some(expected) = mismatch {
            return Err(format!(
                "SessionJoin: the node admitted session {session_id} from {name}, {direction:?}, \
                 with origin {origin:?}, but its link has {expected}"
            ));
        }
        if std::mem::replace(&mut link.admitted, true) {
            return Err(format!(
                "SessionJoin: the node admitted connection {} twice",
                origin.connection
            ));
        }
        Ok(())
    }

    /// Checks `peer`'s admitted session `session_id` against own-9's facts
    /// so far, by the `origin` its handler set, and keeps that origin for
    /// the session's retirement.
    fn join_own9(
        &mut self,
        peer: &ZakuraPeerId,
        session_id: u64,
        origin: Option<SessionOrigin>,
    ) -> Result<(), String> {
        let name = self.name(peer);
        let own9 = self
            .own9
            .as_mut()
            .expect("only a view with own-9 facts joins to them");
        let admitted = format!("the node admitted session {session_id} from {name}");
        let Some(
            origin @ SessionOrigin {
                connection,
                quic_stream: Some(quic_stream),
                generation,
            },
        ) = origin
        else {
            return Err(format!(
                "Own9Join: {admitted} with origin {origin:?}, which names no QUIC stream"
            ));
        };
        let rows: Vec<&Row> = own9
            .rows
            .iter()
            .filter(|row| row.connection == connection && row.quic_stream == quic_stream)
            .collect();
        let [row] = rows[..] else {
            return Err(format!(
                "Own9Join: {admitted}, but {} Session rows are on connection {connection}'s \
                 stream {quic_stream}",
                rows.len()
            ));
        };
        let mismatch = if row.kind != ZAKURA_STREAM_HEADER_SYNC {
            Some(format!("kind {}", row.kind))
        } else if row.peer.as_deref() != Some(name.as_str()) {
            Some(format!("peer {:?}", row.peer))
        } else if row.session_id != session_id {
            Some(format!("session {}", row.session_id))
        } else {
            None
        };
        if let Some(expected) = mismatch {
            return Err(format!(
                "Own9Join: {admitted} on connection {connection}'s stream {quic_stream}, but its \
                 Session row has {expected}"
            ));
        }
        let installs = own9
            .installed
            .iter()
            .filter(|installed| **installed == (connection, generation))
            .count();
        if installs != 1 {
            return Err(format!(
                "Own9Join: {admitted} as generation {generation}, which connection {connection} \
                 installed {installs} times"
            ));
        }
        if generation != session_id {
            return Err(format!(
                "Own9Join: {admitted} as generation {generation}, but the handler installs each \
                 generation under its session ID"
            ));
        }
        own9.origins.insert((peer.clone(), session_id), origin);
        Ok(())
    }

    /// Records `event`'s session fact, if it has one, and checks it.
    fn reactor(&mut self, event: &ReactorEvent) {
        match event {
            ReactorEvent::SessionAdmitted {
                peer,
                session_id,
                direction,
                replaced,
                origin,
            } => {
                self.facts.push(SessionFact::SessionAdmitted {
                    peer: self.name(peer),
                    session_id: *session_id,
                    direction: *direction,
                    replaced: *replaced,
                    origin: *origin,
                });
                let joined = if self.own9.is_some() {
                    self.join_own9(peer, *session_id, *origin)
                } else if self.joined {
                    self.join(peer, *session_id, *direction, *origin)
                } else {
                    Ok(())
                };
                if let Err(why) = joined {
                    self.break_rule(why);
                }
                if let Some(replaced) = replaced {
                    self.depart(peer, *replaced);
                }
            }
            ReactorEvent::SessionEnded {
                peer,
                session_id,
                reason,
            } => {
                self.facts.push(SessionFact::SessionEnded {
                    peer: self.name(peer),
                    session_id: *session_id,
                    reason: (*reason).to_owned(),
                });
                self.depart(peer, *session_id);
            }
            ReactorEvent::Started { request, .. } => {
                if self.departed(&request.peer, request.session_id) {
                    let why = format!(
                        "DepartedSessionWork: request {} started for {}'s session {}, after it \
                         left the reactor's view",
                        request.request_id,
                        self.name(&request.peer),
                        request.session_id
                    );
                    self.break_rule(why);
                }
            }
            ReactorEvent::Retired {
                request:
                    RequestRef {
                        owner: HeaderSyncWorkOwner::BodyRepair(repair),
                        ..
                    },
                ..
            } => {
                self.repairs.remove(repair);
                self.kept.remove(repair);
            }
            ReactorEvent::Dispatched {
                op:
                    PortCall::Prepare {
                        peer,
                        owner: HeaderSyncWorkOwner::BodyRepair(repair),
                        ..
                    },
                ..
            } => {
                self.repairs.insert(*repair, (peer.clone(), false));
            }
            ReactorEvent::Dispatched {
                call,
                op: PortCall::Apply { peer, owner, .. },
            } => {
                let kept = match owner {
                    HeaderSyncWorkOwner::Header(_) => false,
                    HeaderSyncWorkOwner::BodyRepair(repair) => {
                        self.repairs.insert(*repair, (peer.clone(), true));
                        self.kept.remove(repair)
                    }
                };
                if !kept && self.departed(peer, owner.session_id()) {
                    let why = format!(
                        "DepartedSessionWork: call {call} applies {}'s session {}, after it left \
                         the reactor's view",
                        self.name(peer),
                        owner.session_id()
                    );
                    self.break_rule(why);
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests;
