//! The node under test's session view, beside the harness's own links.
//!
//! `header-sync/storage` runs no transport. [`Topology::link`] hands each
//! side an in-memory stream and uses its own counter as the connection, the
//! session ID and the generation. So the harness records its side as the
//! expectation: [`SessionFact::LinkOpened`] when it makes a link, and
//! [`SessionFact::LinkClosed`] when it closes one. The reactor tap records the
//! reactor's side: [`SessionFact::SessionAdmitted`] and
//! [`SessionFact::SessionEnded`]. A [`SessionEvidence`] keeps both in the
//! order they happened and checks two rules as facts arrive.
//!
//! - **Join.** Once the links report here ([`SessionEvidence::expect_links`]),
//!   every admission matches exactly one opened link by its origin's
//!   connection and generation. It must agree with that link on peer,
//!   direction and session ID, so a wrong session ID fails even when the
//!   origin is right. A link is admitted at most once. A refused link has no
//!   admission, so the reverse isn't required.
//! - **Departed sessions stay inert.** A session leaves the reactor's view when
//!   another replaces it or when its disconnect ends it. After that, no
//!   `Started` and no ordinary `Dispatched { Apply }` names it. A late
//!   completion of a call it already started is still legal. So is the apply
//!   of a selected auxiliary repair whose prepare was dispatched before the
//!   session left, which the reactor keeps until its held operation ends.
//!
//! The facts stay out of the observation log, so pinned Plans keep their
//! format. A test reads them from the run's
//! [`Ports`](crate::harness::header_sync::Ports) after the run.
//!
//! [`Topology::link`]: super::Topology::link

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use zakura_header_chain::{BodyWorkOwner, HeaderSyncWorkOwner};
use zakura_network::zakura::{
    observe::{PortCall, ReactorEvent},
    ServicePeerDirection, SessionOrigin, ZakuraPeerId,
};
use zakura_rt::sim::RunId;

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
    /// The harness closed the link on `connection`, the first time it did.
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
    /// Each session that left the reactor's view, by peer and session ID,
    /// with the reactor event count when it left.
    departed: HashMap<(ZakuraPeerId, u64), u64>,
    /// Each auxiliary repair whose prepare was dispatched, with the reactor
    /// event count then.
    repairs: HashMap<BodyWorkOwner, u64>,
    /// Reactor events fed so far.
    events: u64,
    /// The first rule the run broke.
    broken: Option<String>,
}

/// One link the harness made.
struct Link {
    peer: ZakuraPeerId,
    name: String,
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

    /// Turns the join on: the topology reports every link it makes here.
    pub(crate) fn expect_links(&self) {
        self.ledger().joined = true;
    }

    /// Records that the harness linked the node under test to `peer`, named
    /// `name`, on `connection` with `session_id`. `dialer` names the node
    /// that dialed, and `direction` is the node under test's side.
    pub(crate) fn link_opened(
        &self,
        peer: &ZakuraPeerId,
        name: &str,
        connection: u64,
        session_id: u64,
        dialer: &str,
        direction: ServicePeerDirection,
    ) {
        let mut ledger = self.ledger();
        ledger.facts.push(SessionFact::LinkOpened {
            peer: name.to_owned(),
            connection,
            session_id,
            dialer: dialer.to_owned(),
        });
        let link = Link {
            peer: peer.clone(),
            name: name.to_owned(),
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

    /// Records that the harness closed the link on `connection`, unless it
    /// already did.
    pub(crate) fn link_closed(&self, connection: u64) {
        let mut ledger = self.ledger();
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
    /// checking both rules.
    pub(crate) fn reactor(&self, event: &ReactorEvent) {
        self.ledger().reactor(event);
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

    /// `peer`'s name from the links, or its digest.
    fn name(&self, peer: &ZakuraPeerId) -> String {
        self.links
            .values()
            .find(|link| link.peer == *peer)
            .map_or_else(|| super::tap::peer(peer), |link| link.name.clone())
    }

    /// Records `peer`'s session `session_id` as gone from the reactor's view.
    fn depart(&mut self, peer: &ZakuraPeerId, session_id: u64) {
        self.departed
            .entry((peer.clone(), session_id))
            .or_insert(self.events);
    }

    /// Whether work of `peer`'s session `session_id` can't start or apply
    /// now: the session left the view, and the work isn't `repair`, an
    /// auxiliary repair whose prepare was dispatched before it left.
    fn stale(&self, peer: &ZakuraPeerId, session_id: u64, repair: Option<BodyWorkOwner>) -> bool {
        let Some(left) = self.departed.get(&(peer.clone(), session_id)) else {
            return false;
        };
        !repair
            .and_then(|owner| self.repairs.get(&owner))
            .is_some_and(|prepared| prepared < left)
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
        let mismatch = if link.session_id != origin.generation {
            Some(format!("generation {}", link.session_id))
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

    /// Records `event`'s session fact, if it has one, and checks it.
    fn reactor(&mut self, event: &ReactorEvent) {
        self.events += 1;
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
                if self.joined {
                    if let Err(why) = self.join(peer, *session_id, *direction, *origin) {
                        self.break_rule(why);
                    }
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
                if self.stale(&request.peer, request.session_id, None) {
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
            ReactorEvent::Dispatched {
                op:
                    PortCall::Prepare {
                        owner: HeaderSyncWorkOwner::BodyRepair(repair),
                        ..
                    },
                ..
            } => {
                self.repairs.entry(*repair).or_insert(self.events);
            }
            ReactorEvent::Dispatched {
                call,
                op: PortCall::Apply { peer, owner, .. },
            } => {
                let repair = match owner {
                    HeaderSyncWorkOwner::Header(_) => None,
                    HeaderSyncWorkOwner::BodyRepair(repair) => Some(*repair),
                };
                if self.stale(peer, owner.session_id(), repair) {
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
