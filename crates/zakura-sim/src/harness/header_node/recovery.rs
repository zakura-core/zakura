//! The storage harness's recovery phase (fuzz-12 item 5).
//!
//! A `Recover` step names a mirror as the source and ends the hostile
//! prefix. At its pick the scenario's controls stop, every invalidation the
//! scenario made is reconsidered, every hold retires, and from the next pick
//! the run's scheduling follows the recovery policy (see the `plan`
//! module). Recovery is then `Establishing`, which the harness drives
//! itself. At each quiet step, where no task is ready and the control task
//! has carried out every step it took, it offers either:
//!
//! - `peer:node:recovered`, once the source has an admitted session that's
//!   still live, or
//! - `peer:<source>:recovery-relink`, while it has none, unless the node's
//!   readmission cooldown for the source still lasts. Time stops where it
//!   ends.
//! - `peer:node:recovery-unreached`, once [`RECOVERY_RELINKS`] relinks are
//!   carried out and settled without a live source. Its pick ends recovery
//!   unreached, which offers nothing more.
//!
//! A quiet step has handled every admission, refusal, replacement and
//! disconnect an earlier step queued, since an unhandled one leaves its
//! receiving task ready. The `recovered` pick is the conditions point, and
//! writes a [`RecoveryRecord`] of the membership, the source and both
//! nodes' selected chains.

use std::{cell::RefCell, rc::Rc, sync::Arc, time::Duration};

use zakura_header_chain::{Frontier, SuffixWork};
use zakura_network::zakura::ZakuraPeerId;
use zakura_scheduler::SimInstant;
use zakura_storage::header::{adapter::EnginePort, controlled::ControlledPort};

use super::{
    fault::{CooldownEvidence, CooldownFact},
    session::{SessionEvidence, SessionFact},
    tap,
};
use crate::Identity;

/// How long after the later of its submission, or its commit for a held
/// reply, and the `Recover` pick each storage operation must complete.
pub const RECOVERY_BOUND: Duration = Duration::from_secs(1);

/// How many times recovery relinks its source before it gives up.
pub const RECOVERY_RELINKS: u8 = 3;

/// The `recovered` action's local index in the control source.
pub(super) const RECOVERED: u64 = u64::MAX;

/// The `recovery-relink` action's local index in the control source.
pub(super) const RELINK: u64 = u64::MAX - 1;

/// The `recovery-unreached` action's local index in the control source.
pub(super) const UNREACHED: u64 = u64::MAX - 2;

/// What recovery reads: the node's and the mirrors' engines, the node's
/// session view and cooldown facts, its configured readmission cooldown, and
/// each node's peer identity.
pub struct RecoveryInputs {
    /// The node under test's port.
    pub node: Arc<ControlledPort>,
    /// Each mirror's name and engine, in topology order.
    pub mirrors: Vec<(String, Arc<EnginePort>)>,
    /// The node's session view.
    pub sessions: SessionEvidence,
    /// The node's cooldown facts.
    pub cooldowns: CooldownEvidence,
    /// The node's configured readmission cooldown.
    pub cooldown: Duration,
    /// Each node's peer identity and name.
    pub names: Vec<(ZakuraPeerId, String)>,
}

/// One live admitted session of the node: its peer, as the scenario names
/// it, and its session ID.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Member {
    /// The peer.
    pub peer: String,
    /// The session ID.
    pub session_id: u64,
}

/// What the `recovered` pick recorded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryRecord {
    /// Every live admitted session of the node, by peer.
    pub membership: Vec<Member>,
    /// The source's live session.
    pub source: Member,
    /// The node's selected tip and its work after the finalized anchor.
    pub node_tip: (Frontier, SuffixWork),
    /// The source's selected tip and its work after the finalized anchor.
    pub source_tip: (Frontier, SuffixWork),
    /// The source's selected chain from the highest header it shares with
    /// the node's selected chain up to its tip, both ends included.
    pub source_chain: Vec<Frontier>,
    /// How many session facts the view held at the pick, so later ones are
    /// the ones after it.
    pub facts: usize,
}

/// Where recovery is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryState {
    /// The harness drives the conditions. `relinks` counts the relinks it
    /// picked.
    Establishing {
        /// The relinks picked so far.
        relinks: u8,
    },
    /// The conditions held together at the `recovered` pick.
    Recovered(Box<RecoveryRecord>),
    /// Every relink was carried out and settled without a live source, at
    /// the `recovery-unreached` pick.
    Unreached,
}

/// A run's recovery, shared by the control source and the harness's view.
/// Empty until a `Recover` step is picked. Cloning shares it.
#[derive(Clone, Default)]
pub struct RecoveryEvidence(Rc<RefCell<Option<Recovery>>>);

/// One recovery: its source, when it started, and where it is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recovery {
    /// The mirror the `Recover` step named.
    pub source: String,
    /// When the `Recover` step was picked.
    pub started: SimInstant,
    /// Where it is.
    pub state: RecoveryState,
}

impl RecoveryEvidence {
    /// The recovery, if a `Recover` step was picked.
    pub fn get(&self) -> Option<Recovery> {
        self.0.borrow().clone()
    }

    /// When the `Recover` step was picked, if one was.
    pub fn started(&self) -> Option<SimInstant> {
        self.0.borrow().as_ref().map(|recovery| recovery.started)
    }

    /// The record, once the `recovered` pick wrote it.
    pub fn record(&self) -> Option<RecoveryRecord> {
        match &self.0.borrow().as_ref()?.state {
            RecoveryState::Recovered(record) => Some((**record).clone()),
            RecoveryState::Establishing { .. } | RecoveryState::Unreached => None,
        }
    }

    /// Whether recovery ended unreached: every relink settled without a
    /// live source. It offers nothing more then.
    pub fn unreached(&self) -> bool {
        matches!(
            self.0.borrow().as_ref().map(|recovery| &recovery.state),
            Some(RecoveryState::Unreached)
        )
    }

    /// Counts a relink pick and returns the source to relink.
    pub(super) fn relinked(&self) -> String {
        let mut recovery = self.0.borrow_mut();
        let recovery = recovery
            .as_mut()
            .expect("a relink is offered only while recovery establishes");
        if let RecoveryState::Establishing { relinks } = &mut recovery.state {
            *relinks += 1;
        }
        recovery.source.clone()
    }

    /// Ends recovery unreached, at the `recovery-unreached` pick.
    pub(super) fn give_up(&self) {
        if let Some(recovery) = self.0.borrow_mut().as_mut() {
            recovery.state = RecoveryState::Unreached;
        }
    }

    /// Starts recovery from `source` at `started`.
    pub(super) fn start(&self, source: String, started: SimInstant) {
        *self.0.borrow_mut() = Some(Recovery {
            source,
            started,
            state: RecoveryState::Establishing { relinks: 0 },
        });
    }
}

/// The node's live admitted sessions in `facts`: each admitted session whose
/// link is still open, which no later session replaced and whose
/// disconnect hasn't ended it. A session admitted after its link closed
/// isn't live.
pub fn live_sessions(facts: &[SessionFact]) -> Vec<Member> {
    let mut live: Vec<(Member, Option<u64>)> = Vec::new();
    let mut connections: Vec<(u64, u64)> = Vec::new();
    let mut closed: Vec<u64> = Vec::new();
    for fact in facts {
        match fact {
            SessionFact::LinkOpened {
                connection,
                session_id,
                ..
            } => connections.push((*session_id, *connection)),
            SessionFact::SessionAdmitted {
                peer,
                session_id,
                replaced,
                ..
            } => {
                live.retain(|(member, _)| Some(member.session_id) != *replaced);
                let connection = connections
                    .iter()
                    .find(|(session, _)| session == session_id)
                    .map(|(_, connection)| *connection);
                if connection.is_some_and(|connection| closed.contains(&connection)) {
                    continue;
                }
                live.push((
                    Member {
                        peer: peer.clone(),
                        session_id: *session_id,
                    },
                    connection,
                ));
            }
            SessionFact::SessionEnded { session_id, .. } => {
                live.retain(|(member, _)| member.session_id != *session_id);
            }
            SessionFact::LinkClosed { connection, .. } => {
                live.retain(|(_, link)| *link != Some(*connection));
                closed.push(*connection);
            }
            SessionFact::SessionRefused { .. } | SessionFact::SessionRetired { .. } => {}
        }
    }
    let mut members: Vec<Member> = live.into_iter().map(|(member, _)| member).collect();
    members.sort();
    members
}

/// Whether `facts` after the first `from` change the node's membership: a
/// session admitted or ended, or a link closed.
pub fn membership_changed(facts: &[SessionFact], from: usize) -> bool {
    facts.get(from..).unwrap_or_default().iter().any(|fact| {
        matches!(
            fact,
            SessionFact::SessionAdmitted { .. }
                | SessionFact::SessionEnded { .. }
                | SessionFact::LinkClosed { .. }
        )
    })
}

/// Whether `session` is still live after the first `from` of `facts`: no
/// later fact ended it, replaced it or closed its link.
pub fn session_lasts(facts: &[SessionFact], from: usize, session: &Member) -> bool {
    let connection = facts.iter().find_map(|fact| match fact {
        SessionFact::LinkOpened {
            connection,
            session_id,
            ..
        } if *session_id == session.session_id => Some(*connection),
        _ => None,
    });
    !facts
        .get(from..)
        .unwrap_or_default()
        .iter()
        .any(|fact| match fact {
            SessionFact::SessionEnded { session_id, .. } => *session_id == session.session_id,
            SessionFact::SessionAdmitted { replaced, .. } => *replaced == Some(session.session_id),
            SessionFact::LinkClosed {
                connection: closed, ..
            } => Some(*closed) == connection,
            _ => false,
        })
}

impl RecoveryInputs {
    /// The source's live session, if it has one.
    fn source_session(&self, source: &str) -> Option<Member> {
        live_sessions(&self.sessions.facts())
            .into_iter()
            .rev()
            .find(|member| member.peer == source)
    }

    /// Until when the node's readmission cooldown for `source` lasts, as
    /// virtual time since the run's start: the end of the newest cooldown
    /// it started for the source, or the configured cooldown after its
    /// newest refusal of the source, whichever is later.
    fn cooldown_until(&self, source: &str) -> Option<Duration> {
        let (id, _) = self.names.iter().find(|(_, name)| name == source)?;
        cooldown_end(&self.cooldowns.facts(), &tap::peer(id), self.cooldown)
    }

    /// The source's engine.
    fn source_engine(&self, source: &str) -> Option<&EnginePort> {
        self.mirrors
            .iter()
            .find(|(name, _)| name == source)
            .map(|(_, port)| &**port)
    }

    /// The record of the conditions at the `recovered` pick, with `source`'s
    /// live session `session`.
    fn record(&self, source: &str, session: Member) -> Result<RecoveryRecord, String> {
        let facts = self.sessions.facts();
        let engine = self
            .source_engine(source)
            .ok_or_else(|| format!("recovery's source {source} is no mirror"))?;
        let node = self.node.engine();
        let (node_chain, node_snapshot) = (node.selected_path(), node.snapshot());
        let (source_chain, source_snapshot) = (engine.selected_path(), engine.snapshot());
        let shared = node_chain
            .iter()
            .zip(&source_chain)
            .take_while(|(a, b)| a == b)
            .count();
        Ok(RecoveryRecord {
            membership: live_sessions(&facts),
            source: session,
            node_tip: (
                node_snapshot.frontiers.header_best,
                node_snapshot.header_best_score.suffix_work,
            ),
            source_tip: (
                source_snapshot.frontiers.header_best,
                source_snapshot.header_best_score.suffix_work,
            ),
            source_chain: source_chain[shared.saturating_sub(1)..].to_vec(),
            facts: facts.len(),
        })
    }
}

/// Recovery's part of the control source: what it offers at quiet steps and
/// what its picks do.
pub(super) struct Driver {
    pub(super) inputs: RecoveryInputs,
    pub(super) evidence: RecoveryEvidence,
}

impl Driver {
    /// What establishing recovery offers next, or `None` once it's recovered
    /// or before it started.
    fn next(&self, now: SimInstant) -> Option<(String, Next)> {
        let Some(Recovery {
            source,
            state: RecoveryState::Establishing { relinks },
            ..
        }) = self.evidence.get()
        else {
            return None;
        };
        let next = next(
            relinks,
            self.inputs.source_session(&source).is_some(),
            self.inputs.cooldown_until(&source),
            now,
        );
        Some((source, next))
    }

    /// Appends what recovery offers at a quiet step at `now`, where the
    /// control task has carried out every step.
    pub(super) fn quiet(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        match self.next(now) {
            Some((_, Next::Recovered)) => {
                out.push((Identity::peer("node", "recovered"), RECOVERED))
            }
            Some((source, Next::Relink)) => {
                out.push((Identity::peer(&source, "recovery-relink"), RELINK));
            }
            Some((_, Next::GiveUp)) => {
                out.push((Identity::peer("node", "recovery-unreached"), UNREACHED));
            }
            Some((_, Next::WaitUntil(_))) | None => {}
        }
    }

    /// Appends where time must stop: the end of the source's cooldown, while
    /// recovery waits for it.
    pub(super) fn boundaries(&self, now: SimInstant, out: &mut Vec<crate::Boundary>) {
        if let Some((_, Next::WaitUntil(until))) = self.next(now) {
            out.push(crate::Boundary::Opens(until));
        }
    }

    /// Writes the record at the `recovered` pick.
    pub(super) fn recovered(&self) -> Result<(), String> {
        let source = self
            .evidence
            .get()
            .expect("`recovered` is offered only while recovery establishes")
            .source;
        let session = self
            .inputs
            .source_session(&source)
            .expect("`recovered` is offered only while the source has a live session");
        let record = self.inputs.record(&source, session)?;
        if let Some(recovery) = self.evidence.0.borrow_mut().as_mut() {
            recovery.state = RecoveryState::Recovered(Box::new(record));
        }
        Ok(())
    }
}

/// What establishing recovery offers next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Next {
    /// The conditions hold, so `recovered` is offered.
    Recovered,
    /// The source has no live session, so a relink is offered.
    Relink,
    /// The source has no live session, but the node's cooldown for it lasts
    /// until this instant, where time stops.
    WaitUntil(SimInstant),
    /// Every relink is carried out and settled without a live source, so
    /// `recovery-unreached` is offered.
    GiveUp,
}

/// What establishing recovery offers at `now`, after `relinks` relinks,
/// with or without a `live` source session, while the node's readmission
/// cooldown for the source lasts until `cooldown`, as virtual time since the
/// run's start.
pub(super) fn next(relinks: u8, live: bool, cooldown: Option<Duration>, now: SimInstant) -> Next {
    if live {
        return Next::Recovered;
    }
    if relinks >= RECOVERY_RELINKS {
        return Next::GiveUp;
    }
    match cooldown.map(SimInstant::from_start) {
        Some(until) if until > now => Next::WaitUntil(until),
        _ => Next::Relink,
    }
}

/// When the node's readmission cooldown for the peer whose digest is `hex`
/// ends, as virtual time since the run's start, from its cooldown `facts`:
/// the end of the newest cooldown it started for the peer, or the
/// `configured` cooldown after its newest refusal of the peer, whichever is
/// later. `None` if it has neither.
pub(super) fn cooldown_end(
    facts: &[CooldownFact],
    hex: &str,
    configured: Duration,
) -> Option<Duration> {
    facts
        .iter()
        .filter_map(|fact| match fact {
            CooldownFact::Started {
                peer, cooldown, at, ..
            } if peer == hex => Some(at.saturating_add(*cooldown)),
            CooldownFact::Refused { peer, at, .. } if peer == hex => {
                Some(at.saturating_add(configured))
            }
            _ => None,
        })
        .max()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `ms` milliseconds into the run.
    fn at(ms: u64) -> SimInstant {
        SimInstant::from_start(Duration::from_millis(ms))
    }

    #[test]
    fn a_live_source_is_recovered_whatever_else_holds() {
        for relinks in 0..=RECOVERY_RELINKS {
            assert_eq!(
                next(relinks, true, Some(Duration::from_secs(9)), at(0)),
                Next::Recovered
            );
        }
    }

    #[test]
    fn a_refused_source_waits_out_its_cooldown_then_relinks() {
        // Refused at 2 s with a 5 s cooldown: time stops at 7 s, then the
        // harness relinks.
        let facts = [CooldownFact::Refused {
            peer: "aa".into(),
            session_id: 3,
            at: Duration::from_secs(2),
        }];
        let until = cooldown_end(&facts, "aa", Duration::from_secs(5));
        assert_eq!(until, Some(Duration::from_secs(7)));
        assert_eq!(cooldown_end(&facts, "bb", Duration::from_secs(5)), None);
        assert_eq!(next(1, false, until, at(3_000)), Next::WaitUntil(at(7_000)));
        assert_eq!(next(1, false, until, at(7_000)), Next::Relink);
        assert_eq!(next(0, false, None, at(0)), Next::Relink);
    }

    #[test]
    fn the_later_of_a_started_cooldown_and_a_refusal_counts() {
        let facts = [
            CooldownFact::Started {
                peer: "aa".into(),
                origin: 1,
                cooldown: Duration::from_secs(10),
                at: Duration::from_secs(1),
                after: 4,
            },
            CooldownFact::Refused {
                peer: "aa".into(),
                session_id: 2,
                at: Duration::from_secs(3),
            },
        ];
        assert_eq!(
            cooldown_end(&facts, "aa", Duration::from_secs(5)),
            Some(Duration::from_secs(11))
        );
    }

    #[test]
    fn recovery_ends_unreached_only_once_the_third_relink_settles() {
        assert_eq!(next(RECOVERY_RELINKS - 1, false, None, at(0)), Next::Relink);
        assert_eq!(next(RECOVERY_RELINKS, false, None, at(0)), Next::GiveUp);
        assert_eq!(
            next(RECOVERY_RELINKS, false, Some(Duration::from_secs(9)), at(0)),
            Next::GiveUp,
            "no time boundary once every relink is spent"
        );
        let evidence = RecoveryEvidence::default();
        evidence.start("mirror-0".into(), at(0));
        for _ in 0..RECOVERY_RELINKS {
            assert_eq!(evidence.relinked(), "mirror-0");
        }
        // The third relink is picked but not yet settled: still
        // establishing, and its admission still recovers.
        assert!(!evidence.unreached());
        assert_eq!(next(RECOVERY_RELINKS, true, None, at(0)), Next::Recovered);
        // It settled without a live source, and the quiet step's pick ends
        // recovery.
        evidence.give_up();
        assert!(evidence.unreached());
        assert_eq!(evidence.record(), None);
    }
}
